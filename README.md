# Epic FHIR Healthcare Data Integration & Analytics Platform

An end-to-end healthcare data pipeline that extracts FHIR R4 patient data,
transforms it through an automated ETL pipeline, stores it in a SQLite data
warehouse, and delivers clinical insights through a six-page Streamlit dashboard.

## Tech Stack

Python | FHIR R4 | SQLite | Streamlit | Plotly | Pandas | REST APIs | HL7 Standards

## Project Architecture
```
epic_fhir/
├── auth.py                   # FHIR R4 HTTP client with pagination
├── extractors/               # One extractor per FHIR resource
│   ├── patients.py
│   ├── observations.py
│   ├── medications.py
│   └── encounters.py
├── transformers/
│   └── fhir_normalizer.py    # Data cleaning and standardization
├── loaders/
│   └── warehouse.py          # SQLite warehouse with incremental loading
└── analytics/
    └── reports.py            # SQL-based clinical analytics queries

dashboard/
└── app.py                    # Six-page Streamlit analytics application

pipeline.py                   # Pipeline entry point
```

## Features

- Extracts Patient, Observation, MedicationRequest, and Encounter FHIR R4 resources
- Automated ETL pipeline with incremental loading and duplicate prevention
- Data normalization handling encoding issues, date format variations, and invalid references
- Abnormal lab result detection against FHIR reference ranges
- Six-page clinical analytics dashboard with interactive Plotly charts
- Downloadable CSV reports for all analytics tables
- Pipeline run history and success rate monitoring

## Dashboard Pages

| Page | Description |
|------|-------------|
| Population Overview | Patient demographics, gender and age distribution |
| Lab Results | Top lab tests by volume with abnormal rate heatmap |
| Abnormal Results | Flagged observations outside reference ranges |
| Medications | Prescription volume and status breakdown |
| Encounters | Monthly encounter trends by class |
| Pipeline Operations | Run history, success rate, manual pipeline trigger |
| Data Export | CSV download for all four reports |

## Setup
```bash
# Clone the repo
git clone https://github.com/YOURUSERNAME/epic-fhir-platform.git
cd epic-fhir-platform

# Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python pipeline.py

# Launch the dashboard
streamlit run dashboard/app.py
```

## Data Source

This project connects to the HAPI FHIR public R4 server (https://hapi.fhir.org/baseR4),
which provides real FHIR R4 compliant patient data using the same resource structure
and API standard as Epic, Cerner, and all major EHR vendors.