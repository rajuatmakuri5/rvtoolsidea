import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")
st.title("🚀 RVTools Interactive Analyzer")

uploaded_file = st.file_uploader("Upload RVTools Excel Export", type=["xlsx"])

if uploaded_file:

    try:
        vinfo = pd.read_excel(uploaded_file, sheet_name="vInfo")
        vhost = pd.read_excel(uploaded_file, sheet_name="vHost")
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        st.stop()
    vsource = pd.read_excel(uploaded_file, sheet_name="vSource")
    vsource.columns = vsource.columns.str.strip()

    vcenter_name = ""

    try:
        vsource = pd.read_excel(uploaded_file, sheet_name="vSource")
        vsource.columns = vsource.columns.str.strip()

        if "VI SDK Server" in vsource.columns:
            value = vsource["VI SDK Server"].iloc[0]
            if pd.notna(value):
                vcenter_name = str(value).strip()

    except Exception as e:
        # If vSource sheet doesn't exist, ignore
        pass
    from streamlit_tree_select import tree_select
    st.divider()
    st.subheader("🖥 vSphere Inventory")

    col1, col2 = st.columns([1, 2])


    # -----------------------------
    # BUILD UNIQUE TREE
    # -----------------------------
    def build_tree(vinfo_df):
        tree = []

        for dc in sorted(vinfo_df["Datacenter"].dropna().unique()):
            dc_value = f"dc::{dc} ({vcenter_name})"
            dc_node = {"label": f"🏢 {dc} ({vcenter_name}) ", "value": dc_value, "children": []}

            dc_data = vinfo_df[vinfo_df["Datacenter"] == dc]

            for cluster in sorted(dc_data["Cluster"].dropna().unique()):
                cluster_value = f"{dc_value}|cluster::{cluster}"
                cluster_node = {"label": f"🧱 {cluster}", "value": cluster_value, "children": []}

                cluster_data = dc_data[dc_data["Cluster"] == cluster]

                for host in sorted(cluster_data["Host"].dropna().unique()):
                    host_value = f"{cluster_value}|host::{host}"
                    host_node = {"label": f"🖥 {host}", "value": host_value, "children": []}

                    host_data = cluster_data[cluster_data["Host"] == host]

                    for vm in sorted(host_data["VM"].dropna().unique()):
                        vm_value = f"{host_value}|vm::{vm}"
                        vm_node = {"label": f"🟢 {vm}", "value": vm_value}
                        host_node["children"].append(vm_node)

                    cluster_node["children"].append(host_node)

                dc_node["children"].append(cluster_node)

            tree.append(dc_node)

        return tree


    tree_data = build_tree(vinfo)

    with col1:
        selected = tree_select(tree_data)

    # -----------------------------
    # DETAILS PANEL
    # -----------------------------
    with col2:

        if selected["checked"]:
            node = selected["checked"][0]

            if "dc::" in node and "cluster::" not in node:
                #st.header(f"🏢 Datacenter: {node}")
                dc_name = node.split("dc::")[1].split("(")[0].strip()
                #st.header(f"🏢 Datacenter: {dc_name}")
                st.header(f"🏢 Datacenter: {dc_name}")

                dc_data = vinfo[vinfo["Datacenter"] == dc_name]
                st.metric("Total VMs", len(dc_data))
                st.metric("Clusters", dc_data["Cluster"].nunique())

            elif "cluster::" in node and "host::" not in node:
                cluster_name = node.split("cluster::")[1]
                st.header(f"🧱 Cluster: {cluster_name}")

                cluster_hosts = vhost[vhost["Cluster"] == cluster_name]
                cluster_vms = vinfo[vinfo["Cluster"] == cluster_name]

                total_cpu = cluster_hosts["# Cores"].sum()
                total_ram = cluster_hosts["# Memory"].sum()
                total_vcpu = cluster_vms["CPUs"].sum()
                total_vram = cluster_vms["Memory"].sum()

                cpu_ratio = round(total_vcpu / total_cpu, 2)
                mem_ratio = round(total_vram / total_ram, 2)

                st.metric("Hosts", len(cluster_hosts))
                st.metric("Total VMs", len(cluster_vms))
                st.metric("CPU Overcommit", cpu_ratio)
                st.metric("Memory Overcommit", mem_ratio)

            elif "host::" in node and "vm::" not in node:
                host_name = node.split("host::")[1]
                st.header(f"🖥 Host: {host_name}")

                host_data = vhost[vhost["Host"] == host_name].iloc[0]

                st.metric("CPU Model", host_data["CPU Model"])
                st.metric("Total Cores", host_data["# Cores"])
                st.metric("Total Memory (MB)", host_data["# Memory"])
                st.metric("Vendor", host_data["Vendor"])
                st.metric("ESXi Version", host_data["ESX Version"])

            elif "vm::" in node:
                vm_name = node.split("vm::")[1]
                st.header(f"🟢 VM: {vm_name}")

                vm_data = vinfo[vinfo["VM"] == vm_name].iloc[0]

                st.metric("vCPU", vm_data["CPUs"])
                st.metric("vRAM (MB)", vm_data["Memory"])
                st.metric("Power State", vm_data["Powerstate"])
                st.metric("Guest OS", vm_data["OS according to the configuration file"])

        else:
            st.info("Select an object from the inventory tree.")

    # Clean column names
    vhost.columns = vhost.columns.str.strip()
    vinfo.columns = vinfo.columns.str.strip()

    # -----------------------------
    # GLOBAL STATS
    # -----------------------------
    total_vms = len(vinfo)
    total_hosts = vhost["Host"].nunique()
    total_clusters = vhost["Cluster"].nunique()

    total_cpu = vhost["# Cores"].sum()
    total_ram_tb = round(vhost["# Memory"].sum() / 1024, 2)  # Memory is already in GB

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total VMs", total_vms)
    col2.metric("Total Hosts", total_hosts)
    col3.metric("Total Clusters", total_clusters)
    col4.metric("Total Physical CPU Cores", total_cpu)
    col5.metric("Total Physical RAM (GB)", total_ram_tb)

    st.divider()



    # -----------------------------
    # CLUSTER SUMMARY
    # -----------------------------
    st.subheader("📊 Cluster Summary")

    cluster_summary = vhost.groupby("Cluster").agg(
        Hosts=("Host", "count"),
        Total_CPU_Cores=("# Cores", "sum"),
        Total_RAM_MB=("# Memory", "sum")
    ).reset_index()

    st.dataframe(cluster_summary, use_container_width=True)

    st.divider()

    # -----------------------------
    # CLUSTER OVERCOMMIT ANALYSIS
    # -----------------------------
    st.subheader("⚠️ Cluster Overcommit Analysis")

    # Detect VM CPU and Memory columns safely
    vinfo.columns = vinfo.columns.str.strip()

    # Try common VM CPU column names
    vm_cpu_column = None
    for col in vinfo.columns:
        if "cpu" in col.lower():
            vm_cpu_column = col
            break

    # Try common VM memory column names
    vm_ram_column = None
    for col in vinfo.columns:
        if "memory" in col.lower():
            vm_ram_column = col
            break

    if vm_cpu_column is None or vm_ram_column is None:
        st.error("Could not detect VM CPU or Memory columns in vInfo sheet.")
        st.write("vInfo Columns:", vinfo.columns.tolist())
        st.stop()

    # VM aggregation
    cluster_vm = vinfo.groupby("Cluster").agg(
        Total_vCPUs=(vm_cpu_column, "sum"),
        Total_vRAM_MB=(vm_ram_column, "sum")
    ).reset_index()

    # Host aggregation
    cluster_host = vhost.groupby("Cluster").agg(
        Physical_Cores=("# Cores", "sum"),
        Physical_RAM_MB=("# Memory", "sum")
    ).reset_index()

    cluster_analysis = pd.merge(cluster_vm, cluster_host, on="Cluster")

    # Calculate ratios
    cluster_analysis["CPU_Overcommit_Ratio"] = (
            cluster_analysis["Total_vCPUs"] /
            cluster_analysis["Physical_Cores"]
    ).round(2)

    cluster_analysis["Memory_Overcommit_Ratio"] = (
            cluster_analysis["Total_vRAM_MB"] /
            cluster_analysis["Physical_RAM_MB"]
    ).round(2)


    # Risk Classification
    def classify_cpu(ratio):
        if ratio > 4:
            return "🔴 High"
        elif ratio > 2:
            return "🟡 Moderate"
        else:
            return "🟢 Healthy"


    def classify_memory(ratio):
        if ratio > 1.5:
            return "🔴 High"
        elif ratio > 1:
            return "🟡 Moderate"
        else:
            return "🟢 Healthy"


    cluster_analysis["CPU_Risk"] = cluster_analysis["CPU_Overcommit_Ratio"].apply(classify_cpu)
    cluster_analysis["Memory_Risk"] = cluster_analysis["Memory_Overcommit_Ratio"].apply(classify_memory)

    st.dataframe(cluster_analysis, use_container_width=True)
