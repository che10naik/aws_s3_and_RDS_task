from datetime import datetime
from pathlib import Path
import json
import logging
import pandas as pd
from rds_handler import RDSTableHandler


# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# LocalParquetImporter Class
class LocalParquetImporter:
    """
    Reads local Parquet chunk files and inserts data into RDS using RDSTableHandler.
    Automatically skips already imported files using a manifest tracker.
    Limits the total imported rows to a configured number (default 10,000).
    """

    def __init__(self, config_path="config.ini", row_limit=10000):
        self.handler = RDSTableHandler(config_path)
        self.config = self.handler.config

        pq_cfg = self.config["PARQUET_CONVERSION"]
        rds_cfg = self.config["RDS"]

        self.input_dir = Path(pq_cfg.get("output_dir", "data/parquet_chunks"))
        self.table_name = rds_cfg.get("table_name", "employee_chetanN")
        self.batch_size = int(rds_cfg.get("batch_size", 10000))

        # Import only first 10,000 rows total
        self.row_limit = row_limit

        # Manifest to track completed imports
        self.manifest_dir = Path("data/manifests")
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.import_manifest_file = self.manifest_dir / "import_manifest.json"

        # Load existing import log
        self.imported_files = self.load_import_manifest()

    # Manifest Handling
    def load_import_manifest(self):
        """Load list of already imported Parquet files."""
        if self.import_manifest_file.exists():
            try:
                with open(self.import_manifest_file, "r") as f:
                    data = json.load(f)
                    logger.info(f"Loaded import manifest with {len(data)} entries.")
                    return {entry["file"]: entry for entry in data}
            except Exception as e:
                logger.warning(f"Failed to read manifest file: {e}")
        return {}

    def update_import_manifest(self, file_name, row_count):
        """Update manifest after each successful import."""
        self.imported_files[file_name] = {
            "file": file_name,
            "rows_imported": row_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(self.import_manifest_file, "w") as f:
            json.dump(list(self.imported_files.values()), f, indent=4)
        logger.info(f"Manifest updated: {file_name} marked as imported.")

    # File Import Logic
    def get_local_parquet_files(self):
        """Find all parquet files in the configured directory."""
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.input_dir}")

        parquet_files = sorted(self.input_dir.glob("*.parquet"))
        if not parquet_files:
            logger.warning("No Parquet files found for import.")
        return parquet_files

    def import_to_rds(self):
        """Main method to import Parquet files into RDS table."""
        self.handler.create_table_if_not_exists()

        # Get current number of rows in the table
        current_rows = self.handler.count_rows()
        if current_rows >= self.row_limit:
            logger.info(f"Table already has {current_rows} rows. Skipping import.")
            return

        parquet_files = self.get_local_parquet_files()
        total_rows_inserted = 0
        skipped_files = 0

        for file_path in parquet_files:
            file_name = file_path.name

            # Stop if row limit reached
            if current_rows + total_rows_inserted >= self.row_limit:
                logger.info(f"Reached total import limit of {self.row_limit} rows. Stopping import.")
                break

            # Skip already imported files
            if file_name in self.imported_files:
                logger.info(f"Skipping already imported file: {file_name}")
                skipped_files += 1
                continue

            logger.info(f"Importing file: {file_name}")
            try:
                df = pd.read_parquet(file_path)
                if df.empty:
                    logger.warning(f"Skipping empty file: {file_name}")
                    continue

                # Limit remaining rows to reach row_limit
                remaining = self.row_limit - (current_rows + total_rows_inserted)
                if len(df) > remaining:
                    df = df.head(remaining)

                records = df.to_dict(orient="records")
                inserted = self.handler.insert_many(records)
                total_rows_inserted += inserted

                # Mark this file as imported
                self.update_import_manifest(file_name, inserted)

                logger.info(f"Inserted {inserted} records from {file_name}")

                if current_rows + total_rows_inserted >= self.row_limit:
                    logger.info(f"Reached total import limit of {self.row_limit} rows.")
                    break

            except Exception as e:
                logger.error(f"Error processing {file_name}: {e}")

        logger.info(
            f"Import completed successfully.\n"
            f"Total new rows inserted: {total_rows_inserted}\n"
            f"Skipped previously imported files: {skipped_files}"
)



# Entry Point
if __name__ == "__main__":
    importer = LocalParquetImporter(row_limit=10000)
    importer.import_to_rds()
