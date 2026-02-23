import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from fhir_platform.analytics.reports import ClinicalReports
from fhir_platform.loaders.warehouse import FHIRWarehouse
from pipeline import run_pipeline

st.set_page_config(
    page_title="FHIR Clinical Analytics Platform",
    page_icon="🏥",
    layout="wide"
)

reports = ClinicalReports()
warehouse = FHIRWarehouse()


def render_header():
    st.title("FHIR Clinical Analytics Platform")
    st.caption("Real-time analytics on FHIR R4 patient data")
    st.divider()


def render_kpi_row():
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Patients", warehouse.get_record_count("patients"))
    col2.metric("Total Observations", warehouse.get_record_count("observations"))
    col3.metric("Total Medications", warehouse.get_record_count("medications"))
    col4.metric("Total Encounters", warehouse.get_record_count("encounters"))


def page_overview():
    render_header()
    st.subheader("Population Overview")
    render_kpi_row()
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Patient Gender Distribution")
        df = reports.patient_demographics()
        if not df.empty:
            fig = px.pie(df, names="gender", values="total_patients",
                        color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Age Group Distribution")
        df = reports.age_distribution()
        if not df.empty:
            fig = px.bar(df, x="age_group", y="total_patients",
                        color="age_group",
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


def page_lab_results():
    render_header()
    st.subheader("Lab Results Analysis")

    df = reports.lab_result_summary()
    if not df.empty:
        st.subheader("Top Lab Tests by Volume")
        fig = px.bar(df.head(10), x="total_results", y="test_name",
                    orientation="h",
                    color="abnormal_rate",
                    color_continuous_scale="RdYlGn_r",
                    labels={"total_results": "Total Results",
                            "test_name": "Test",
                            "abnormal_rate": "Abnormal Rate %"})
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Lab Results Detail")
        st.dataframe(df, use_container_width=True)


def page_abnormal_results():
    render_header()
    st.subheader("Abnormal Result Detection")

    df = reports.abnormal_results()
    if df.empty:
        st.info("No abnormal results detected in current dataset.")
        st.write("This is expected with public test data that lacks reference ranges.")
    else:
        st.error(f"{len(df)} abnormal results detected")
        st.dataframe(df, use_container_width=True)


def page_medications():
    render_header()
    st.subheader("Medication Analysis")

    df = reports.medication_summary()
    if not df.empty:
        st.subheader("Top Medications by Prescription Volume")
        fig = px.bar(df.head(15), x="total_prescriptions", y="medication_name",
                    orientation="h",
                    color="active",
                    color_continuous_scale="Blues",
                    labels={"total_prescriptions": "Total Prescriptions",
                            "medication_name": "Medication",
                            "active": "Active Count"})
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Medication Status Breakdown")
        status_data = pd.DataFrame({
            "Status": ["Active", "Cancelled", "Completed"],
            "Count": [
                df["active"].sum(),
                df["cancelled"].sum(),
                df["completed"].sum()
            ]
        })
        fig2 = px.pie(status_data, names="Status", values="Count",
                     color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig2, use_container_width=True)


def page_encounters():
    render_header()
    st.subheader("Encounter Trends")

    df = reports.encounter_trends()
    if not df.empty:
        st.subheader("Monthly Encounter Volume")
        fig = px.line(df, x="month", y="total_encounters",
                     markers=True,
                     labels={"month": "Month",
                             "total_encounters": "Total Encounters"})
        fig.update_traces(line_color="#2196F3", line_width=2)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Encounter Class Breakdown")
        fig2 = px.bar(df, x="month",
                     y=["ambulatory", "inpatient", "emergency"],
                     barmode="stack",
                     labels={"month": "Month", "value": "Count",
                             "variable": "Class"})
        st.plotly_chart(fig2, use_container_width=True)


def page_pipeline():
    render_header()
    st.subheader("Pipeline Operations")

    if st.button("Run Pipeline Now", type="primary"):
        with st.spinner("Running pipeline..."):
            run_pipeline(max_records=100)
        st.success("Pipeline completed successfully")
        st.rerun()

    st.subheader("Pipeline Run History")
    df = reports.pipeline_run_history()
    if not df.empty:
        st.dataframe(df, use_container_width=True)

        success = df[df["status"] == "success"].shape[0]
        total = df.shape[0]
        rate = round(success / total * 100, 1) if total > 0 else 0
        st.metric("Pipeline Success Rate", f"{rate}%")


def page_data_export():
    render_header()
    st.subheader("Download Reports")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Patient Demographics Report**")
        df = reports.patient_demographics()
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False),
            file_name="patient_demographics.csv",
            mime="text/csv"
        )

    with col2:
        st.write("**Lab Results Report**")
        df = reports.lab_result_summary()
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False),
            file_name="lab_results.csv",
            mime="text/csv"
        )

    col3, col4 = st.columns(2)

    with col3:
        st.write("**Medication Summary Report**")
        df = reports.medication_summary()
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False),
            file_name="medication_summary.csv",
            mime="text/csv"
        )

    with col4:
        st.write("**Encounter Trends Report**")
        df = reports.encounter_trends()
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False),
            file_name="encounter_trends.csv",
            mime="text/csv"
        )


pages = {
    "Population Overview": page_overview,
    "Lab Results": page_lab_results,
    "Abnormal Results": page_abnormal_results,
    "Medications": page_medications,
    "Encounters": page_encounters,
    "Pipeline Operations": page_pipeline,
    "Data Export": page_data_export,
}

with st.sidebar:
    st.title("Navigation")
    selection = st.radio("Go to", list(pages.keys()))

pages[selection]()

