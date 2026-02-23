import sqlite3
import logging
from pathlib import Path
from config import DATABASE_PATH

logger = logging.getLogger(__name__)


class FHIRWarehouse:
    """
    Manages the SQLite data warehouse.
    Handles table creation, incremental loading, and deduplication.
    """

    def __init__(self):
        self.db_path = DATABASE_PATH
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize_tables()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _initialize_tables(self):
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id TEXT PRIMARY KEY,
                    full_name TEXT,
                    gender TEXT,
                    birth_date TEXT,
                    city TEXT,
                    state TEXT,
                    country TEXT,
                    marital_status TEXT,
                    language TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS observations (
                    observation_id TEXT PRIMARY KEY,
                    patient_id TEXT,
                    status TEXT,
                    category TEXT,
                    code TEXT,
                    display TEXT,
                    value REAL,
                    unit TEXT,
                    reference_low REAL,
                    reference_high REAL,
                    is_abnormal INTEGER,
                    effective_date TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS medications (
                    medication_id TEXT PRIMARY KEY,
                    patient_id TEXT,
                    status TEXT,
                    intent TEXT,
                    medication_code TEXT,
                    medication_name TEXT,
                    authored_on TEXT,
                    dose_value REAL,
                    dose_unit TEXT,
                    route TEXT,
                    frequency TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS encounters (
                    encounter_id TEXT PRIMARY KEY,
                    patient_id TEXT,
                    status TEXT,
                    class TEXT,
                    type TEXT,
                    start TEXT,
                    end TEXT,
                    provider TEXT,
                    reason TEXT,
                    location TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resource TEXT,
                    records_extracted INTEGER,
                    records_loaded INTEGER,
                    status TEXT
                );
            """)
        logger.info("Warehouse tables initialized")

    def load(self, table: str, records: list[dict], id_field: str) -> int:
        """
        Incrementally loads records into the warehouse.
        Skips records that already exist based on the id_field.
        Returns count of newly inserted records.
        """
        if not records:
            return 0

        loaded = 0
        columns = list(records[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        col_names = ", ".join(columns)

        sql = f"""
            INSERT OR IGNORE INTO {table} ({col_names})
            VALUES ({placeholders})
        """

        with self._get_connection() as conn:
            for record in records:
                values = [record.get(c) for c in columns]
                cursor = conn.execute(sql, values)
                loaded += cursor.rowcount

        logger.info(f"Loaded {loaded} new records into {table}")
        return loaded

    def log_pipeline_run(self, resource: str, extracted: int,
                         loaded: int, status: str):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO pipeline_runs
                    (resource, records_extracted, records_loaded, status)
                VALUES (?, ?, ?, ?)
            """, (resource, extracted, loaded, status))

    def get_record_count(self, table: str) -> int:
        with self._get_connection() as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]