import sqlite3
import pandas as pd
import logging
from config import DATABASE_PATH

logger = logging.getLogger(__name__)


class ClinicalReports:
    """
    SQL-based analytics reports on the FHIR warehouse data.
    Each method returns a pandas DataFrame ready for dashboard consumption.
    """

    def __init__(self):
        self.db_path = DATABASE_PATH

    def _query(self, sql: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn)

    def patient_demographics(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                gender,
                COUNT(*) as total_patients,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM patients
            WHERE gender IS NOT NULL
            GROUP BY gender
            ORDER BY total_patients DESC
        """)

    def age_distribution(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                CASE
                    WHEN CAST(strftime('%Y', 'now') AS INT) -
                         CAST(substr(birth_date, 1, 4) AS INT) < 18 THEN 'Under 18'
                    WHEN CAST(strftime('%Y', 'now') AS INT) -
                         CAST(substr(birth_date, 1, 4) AS INT) BETWEEN 18 AND 35 THEN '18-35'
                    WHEN CAST(strftime('%Y', 'now') AS INT) -
                         CAST(substr(birth_date, 1, 4) AS INT) BETWEEN 36 AND 55 THEN '36-55'
                    WHEN CAST(strftime('%Y', 'now') AS INT) -
                         CAST(substr(birth_date, 1, 4) AS INT) > 55 THEN 'Over 55'
                    ELSE 'Unknown'
                END as age_group,
                COUNT(*) as total_patients
            FROM patients
            GROUP BY age_group
            ORDER BY total_patients DESC
        """)

    def lab_result_summary(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                display as test_name,
                COUNT(*) as total_results,
                ROUND(AVG(value), 2) as avg_value,
                ROUND(MIN(value), 2) as min_value,
                ROUND(MAX(value), 2) as max_value,
                unit,
                SUM(is_abnormal) as abnormal_count,
                ROUND(SUM(is_abnormal) * 100.0 / COUNT(*), 1) as abnormal_rate
            FROM observations
            WHERE value IS NOT NULL
            AND display IS NOT NULL
            GROUP BY display, unit
            ORDER BY total_results DESC
            LIMIT 20
        """)

    def abnormal_results(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                o.observation_id,
                o.patient_id,
                o.display as test_name,
                o.value,
                o.unit,
                o.reference_low,
                o.reference_high,
                o.effective_date
            FROM observations o
            WHERE o.is_abnormal = 1
            ORDER BY o.effective_date DESC
        """)

    def medication_summary(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                medication_name,
                COUNT(*) as total_prescriptions,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM medications
            WHERE medication_name IS NOT NULL
            GROUP BY medication_name
            ORDER BY total_prescriptions DESC
            LIMIT 20
        """)

    def encounter_trends(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                substr(start, 1, 7) as month,
                COUNT(*) as total_encounters,
                SUM(CASE WHEN class = 'AMB' THEN 1 ELSE 0 END) as ambulatory,
                SUM(CASE WHEN class = 'IMP' THEN 1 ELSE 0 END) as inpatient,
                SUM(CASE WHEN class = 'EMER' THEN 1 ELSE 0 END) as emergency
            FROM encounters
            WHERE start IS NOT NULL
            GROUP BY month
            ORDER BY month DESC
            LIMIT 12
        """)

    def pipeline_run_history(self) -> pd.DataFrame:
        return self._query("""
            SELECT
                run_at,
                resource,
                records_extracted,
                records_loaded,
                status
            FROM pipeline_runs
            ORDER BY run_at DESC
            LIMIT 20
        """)

