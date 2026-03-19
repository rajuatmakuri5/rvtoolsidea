import io
import os
import re
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

# ---------------------------------------------------------
# HCL configuration
# ---------------------------------------------------------

CPU_SERIES_COL = "CPU Series"
SUPPORTED_RELEASES_COL = "Supported Releases"  # adjust if different in your HCL

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def normalize_cpu_model(cpu_model: str) -> Tuple[str, Optional[str]]:
    """
    Normalize an RVTools CPU Model string to a Broadcom-style CPU Series.

    Example:
      "Intel(R) Xeon(R) Gold 5317 CPU @ 3.00GHz" -> "Intel Xeon Gold 5300"
      "Intel(R) Xeon(R) Gold 6426Y" -> "Intel Xeon Gold 6400"
    """
    original = str(cpu_model)
    s = original

    # Remove (R), "CPU" word, extra spaces
    s = s.replace("(R)", "").replace("CPU", "")
    s = " ".join(s.split())

    # Remove frequency suffixes like "@ 3.00GHz"
    s = re.sub(r"@\s*\d+(\.\d+)?\s*GHz", "", s, flags=re.IGNORECASE)
    s = " ".join(s.split())

    # Regex to find something like "Intel Xeon Gold 5317"
    m = re.search(
        r"(Intel)\s+Xeon\s+(Gold|Silver|Bronze|Platinum)\s+(\d+)",
        s,
        flags=re.IGNORECASE,
    )
    if not m:
        # Fallback: return cleaned string as-is
        return (
            s.strip(),
            f"Could not confidently map SKU for '{original}', using '{s.strip()}' as CPU Series",
        )

    vendor = m.group(1).strip()
    family = m.group(2).strip()
    sku_str = m.group(3).strip()

    # Basic rule: take the first two digits as the "hundreds" series
    if len(sku_str) >= 2:
        hundreds = int(sku_str[:2]) * 100
    else:
        hundreds = int(sku_str) * 100

    series = f"{vendor} Xeon {family} {hundreds}"
    return series, None

def resolve_esxi_support(
    cpu_series: str, hcl_df: pd.DataFrame, esxi_versions: List[str]
) -> Tuple[Dict[str, str], bool]:
    """
    For a given CPU Series, check ESXi version support in the HCL.

    Returns:
      (support_map, not_found)
      - support_map: { "ESXi 9.0": "Yes"/"No"/"Unknown", ... }
      - not_found: True if CPU Series was not found at all in the HCL.
    """
    mask = hcl_df[CPU_SERIES_COL].astype(str).str.contains(
        cpu_series, case=False, na=False
    )
    subset = hcl_df[mask]

    if subset.empty:
        return {v: "Unknown" for v in esxi_versions}, True

    results: Dict[str, str] = {}
    for v in esxi_versions:
        found = subset[SUPPORTED_RELEASES_COL].astype(str).str.contains(
            v, case=False, na=False
        ).any()
        results[v] = "Yes" if found else "No"
    return results, False

def extract_vcenter_name_from_vsource(xls: pd.ExcelFile) -> str:
    """
    Get vCenter name from vSource sheet, column 'VI SDK Server', first row.
    Returns empty string if not found.
    """
    try:
        vsource = pd.read_excel(xls, sheet_name="vSource")
        vsource.columns = vsource.columns.str.strip()
        if "VI SDK Server" in vsource.columns and not vsource.empty:
            value = vsource["VI SDK Server"].iloc[0]
            if pd.notna(value):
                return str(value).strip()
    except Exception:
        pass
    return ""  # return empty if not found

def run_cpu_esxi_summary_for_vhost(
    vhost: pd.DataFrame,
    hcl_df: pd.DataFrame,
    esxi_versions: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Run CPU / ESXi summary using a vHost DataFrame and the HCL DataFrame.
    """
    if esxi_versions is None:
        esxi_versions = ["ESXi 9.0", "ESXi 9.1"]

    expected_cols = {"CPU Model", "Vendor", "Model"}
    missing = expected_cols - set(vhost.columns)
    if missing:
        raise ValueError(f"vHost sheet is missing columns: {missing}")

    subset = vhost[["CPU Model", "Vendor", "Model"]].copy()
    subset = subset.dropna(how="all")

    grouped = (
        subset.groupby(["CPU Model", "Vendor", "Model"], dropna=False)
        .size()
        .reset_index(name="Host count")
    )
    tuples = grouped.to_dict(orient="records")

    summary_rows: List[Dict] = []
    assumptions: List[str] = []

    for row in tuples:
        cpu_model = row["CPU Model"]
        vendor = row["Vendor"]
        model = row["Model"]
        host_count = row["Host count"]

        cpu_series, note = normalize_cpu_model(cpu_model)
        if note:
            assumptions.append(note)

        support_map, not_found = resolve_esxi_support(cpu_series, hcl_df, esxi_versions)
        if not_found:
            assumptions.append(
                f"No HCL entries found for CPU Series '{cpu_series}' (from '{cpu_model}')"
            )

        summary_row = {
            "CPU Model": cpu_model,
            "Vendor": vendor,
            "Model": model,
            "CPU Series": cpu_series,
            "Host count": host_count,
        }
        for v in esxi_versions:
            summary_row[v] = support_map.get(v, "Unknown")

        summary_rows.append(summary_row)

    cpu_df = pd.DataFrame(summary_rows)

    # Deduplicate assumptions
    seen = set()
    uniq_assumptions = []
    for a in assumptions:
        if a not in seen:
            seen.add(a)
            uniq_assumptions.append(a)

    return cpu_df, uniq_assumptions

def build_cluster_host_mapping(
    vhost: pd.DataFrame, hcl_df: pd.DataFrame, esxi_versions: List[str]
) -> Tuple[pd.DataFrame, List[str]]:
    """
    From vHost DataFrame, build DataFrame:
      Cluster | Host | ESXi 9.0 | ESXi 9.1

    ESXi columns are derived based on each host's CPU Model via the same
    CPU Series → HCL lookup used in the CPU summary.
    """
    host_col_candidates = ["Host", "Hostname", "ESX host", "Name"]
    host_col = None
    for c in host_col_candidates:
        if c in vhost.columns:
            host_col = c
            break
    if host_col is None:
        raise ValueError(
            f"Could not find a host column in vHost sheet. "
            f"Looked for: {host_col_candidates}"
        )
    if "Cluster" not in vhost.columns:
        raise ValueError("vHost sheet does not contain a 'Cluster' column.")
    if "CPU Model" not in vhost.columns:
        raise ValueError("vHost sheet does not contain a 'CPU Model' column.")

    df = vhost.rename(columns={host_col: "Host"})
    tmp = df[["Cluster", "Host", "CPU Model"]].copy()
    tmp = tmp.dropna(subset=["Cluster", "Host"])

    tmp["Cluster"] = tmp["Cluster"].astype(str).str.strip()
    tmp["Host"] = tmp["Host"].astype(str).str.strip()
    tmp["CPU Model"] = tmp["CPU Model"].astype(str).str.strip()

    rows = []
    assumptions: List[str] = []

    # Cache CPU Model → (CPU Series, support_map) so we don't look up repeatedly
    cpu_cache: Dict[str, Tuple[str, Dict[str, str], bool]] = {}

    grouped = tmp.groupby("Cluster")
    for cluster, group in grouped:
        # Use unique hosts within cluster
        for _, r in group[["Host", "CPU Model"]].drop_duplicates().iterrows():
            host_name = r["Host"]
            cpu_model = r["CPU Model"]

            if cpu_model not in cpu_cache:
                cpu_series, note = normalize_cpu_model(cpu_model)
                if note:
                    assumptions.append(note)
                support_map, not_found = resolve_esxi_support(
                    cpu_series, hcl_df, esxi_versions
                )
                if not_found:
                    assumptions.append(
                        f"No HCL entries found for CPU Series '{cpu_series}' (from '{cpu_model}')"
                    )
                cpu_cache[cpu_model] = (cpu_series, support_map, not_found)

            cpu_series, support_map, _ = cpu_cache[cpu_model]

            row = {
                "Cluster": cluster,
                "Host": host_name,
                "CPU Model": cpu_model,
                "CPU Series": cpu_series,
            }
            for v in esxi_versions:
                row[v] = support_map.get(v, "Unknown")

            rows.append(row)

    out = pd.DataFrame(rows).sort_values(by=["Cluster", "Host"]).reset_index(drop=True)

    # Deduplicate assumptions
    seen = set()
    uniq_assumptions = []
    for a in assumptions:
        if a not in seen:
            seen.add(a)
            uniq_assumptions.append(a)

    return out, uniq_assumptions

def create_per_vcenter_excel(cpu_df: pd.DataFrame, cluster_df: pd.DataFrame) -> bytes:
    """
    Create an Excel file in memory with two sheets:
      - CPU_ESXi_Summary
      - Cluster_Host_Mapping
    Return as bytes for Streamlit download.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        cpu_df.to_excel(writer, sheet_name="CPU_ESXi_Summary", index=False)
        cluster_df.to_excel(writer, sheet_name="Cluster_Host_Mapping", index=False)
    buffer.seek(0)
    return buffer.getvalue()

# ---------------------------------------------------------
# Streamlit App
# ---------------------------------------------------------

st.set_page_config(page_title="RVTools ESXi 9.x Analyzer", layout="wide")
st.title("🚀 RVTools ESXi 9.x Compatibility Analyzer")

st.markdown(
    """
Upload up to **5 RVTools Excel exports** and **one Broadcom Systems/Servers HCL** file.  
For each RVTools file, this app will:

1. Detect the **vCenter name** from the `vSource` sheet.  
2. Generate a **CPU → ESXi 9.x support summary** (from `vHost` + HCL).  
3. Generate a **Cluster → ESXi Host mapping** with **ESXi 9.0/9.1 support** per host (from `vHost` + HCL).  
4. Show the results in this UI and provide an **Excel download** per vCenter.
"""
)

col_left, col_right = st.columns(2)

with col_left:
    rvtools_files = st.file_uploader(
        "Upload RVTools Excel exports (max 5)",
        type=["xlsx"],
        accept_multiple_files=True,
    )
    if rvtools_files and len(rvtools_files) > 5:
        st.warning("You uploaded more than 5 files; only the first 5 will be used.")
        rvtools_files = rvtools_files[:5]

with col_right:
    hcl_file = st.file_uploader(
        "Upload Broadcom Systems/Servers HCL (CSV or Excel)",
        help="Please download the latest compatibility file from: https://compatibilityguide.broadcom.com/search?program=server&persona=live&column=partnerName&order=asc",
        type=["csv", "xlsx", "xls"],
    )

esxi_versions = ["ESXi 9.0", "ESXi 9.1"]

if st.button("Run Analysis"):

    if not rvtools_files:
        st.error("Please upload at least one RVTools Excel file.")
        st.stop()
    if not hcl_file:
        st.error("Please upload the Broadcom HCL file.")
        st.stop()

    # Load HCL into DataFrame
    try:
        hcl_ext = os.path.splitext(hcl_file.name)[1].lower()
        if hcl_ext in [".csv", ".txt"]:
            hcl_df = pd.read_csv(hcl_file)
        else:
            hcl_df = pd.read_excel(hcl_file)
        hcl_df.columns = hcl_df.columns.str.strip()
        missing = {CPU_SERIES_COL, SUPPORTED_RELEASES_COL} - set(hcl_df.columns)
        if missing:
            st.error(
                f"HCL file missing required columns: {missing}. "
                f"Expected at least: {CPU_SERIES_COL!r}, {SUPPORTED_RELEASES_COL!r}"
            )
            st.stop()
    except Exception as e:
        st.error(f"Error reading HCL file: {e}")
        st.stop()

    st.success("Files loaded. Running analysis.")

    # counter to ensure unique names when vCenter not found
    unknown_counter = 0

    for idx, uploaded_file in enumerate(rvtools_files):
        st.divider()
        st.subheader(f"📁 RVTools File: {uploaded_file.name}")

        # Load Excel once, reuse across sheets
        try:
            xls = pd.ExcelFile(uploaded_file)
        except Exception as e:
            st.error(f"Error reading Excel file {uploaded_file.name}: {e}")
            continue

        # Get vCenter name from vSource
        raw_vcenter_name = extract_vcenter_name_from_vsource(xls)
        if raw_vcenter_name:
            vcenter_name = raw_vcenter_name
        else:
            unknown_counter += 1
            vcenter_name = f"Unknown_vCenter_{unknown_counter}"

        st.markdown(f"**vCenter:** `{vcenter_name}`")

        # Load vHost
        try:
            vhost = pd.read_excel(xls, sheet_name="vHost")
            vhost.columns = vhost.columns.str.strip()
        except Exception as e:
            st.error(f"Error reading vHost sheet in {uploaded_file.name}: {e}")
            continue

        # CPU / ESXi summary
        try:
            cpu_df, assumptions_cpu = run_cpu_esxi_summary_for_vhost(
                vhost=vhost,
                hcl_df=hcl_df,
                esxi_versions=esxi_versions,
            )
        except Exception as e:
            st.error(f"Error generating CPU/ESXi summary for {uploaded_file.name}: {e}")
            continue

        # Cluster → Host mapping with ESXi support
        try:
            cluster_df, assumptions_cluster = build_cluster_host_mapping(
                vhost=vhost,
                hcl_df=hcl_df,
                esxi_versions=esxi_versions,
            )
        except Exception as e:
            st.error(f"Error generating cluster/host mapping for {uploaded_file.name}: {e}")
            continue

        # Show results in expanders
        with st.expander(f"🧠 CPU / ESXi 9.x Summary - {vcenter_name}", expanded=True):
            st.dataframe(cpu_df, use_container_width=True)

        with st.expander(
            f"🧱 Cluster → Host Mapping (with ESXi 9.x) - {vcenter_name}",
            expanded=False,
        ):
            st.dataframe(cluster_df, use_container_width=True)

        all_assumptions = assumptions_cpu + assumptions_cluster
        if all_assumptions:
            with st.expander(f"ℹ️ Assumptions / Notes - {vcenter_name}", expanded=False):
                seen_a = set()
                for a in all_assumptions:
                    if a not in seen_a:
                        seen_a.add(a)
                        st.write(f"- {a}")

        # Create Excel for download
        excel_bytes = create_per_vcenter_excel(cpu_df, cluster_df)

        # Generate a safe base for the filename
        safe_vcenter_name = (
            vcenter_name.replace(" ", "_").replace(".", "_").replace("/", "_")
        )

        # Use a unique key per button to avoid StreamlitDuplicateElementId
        st.download_button(
            label=f"⬇️ Download Excel report for {vcenter_name}",
            data=excel_bytes,
            file_name=f"{safe_vcenter_name}_ESXi9_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_{safe_vcenter_name}_{idx}",
        )
else:
    st.info("Upload files and click **Run Analysis** to start.")
