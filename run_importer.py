import sys
import logging
from upload_aws import S3Uploader
from data_importer import LocalParquetImporter

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Factory Pattern
class DataImporterFactory:
    """Factory to create data importer instances based on type."""

    @staticmethod
    def get_importer(import_type: str):
        """Return an importer instance based on the import_type."""
        import_type = import_type.strip().upper()
        if import_type == "S3":
            return S3ImporterWrapper()
        elif import_type == "RDS":
            return RDSImporterWrapper()
        else:
            raise ValueError("import_type must be 'S3' or 'RDS'")

# Concrete Wrapper Classes
class S3ImporterWrapper:
    """Wrapper around S3Uploader to match import_data interface."""

    def import_data(self):
        """Main method to upload Parquet files to S3."""
        try:
            uploader = S3Uploader()
            latest_manifest = uploader.get_latest_manifest()
            logger.info(f"Uploading files from manifest: {latest_manifest.name}")
            summary = uploader.upload_file_from_manifest(latest_manifest)
            logger.info(f"Upload summary saved at: {summary}")
            return summary
        except Exception as e:
            logger.error(f"S3 import failed: {e}")
            raise RuntimeError(f"S3 import failed: {e}")

class RDSImporterWrapper:
    """Wrapper around LocalParquetImporter to match import_data interface."""

    def import_data(self):
        """Main method to import Parquet files into RDS table."""
        try:
            importer = LocalParquetImporter(row_limit=10000)
            importer.import_to_rds()
            logger.info(f"RDS import completed successfully. Table: {importer.handler.table_name}")
            return f"RDS import completed. Table: {importer.handler.table_name}"
        except Exception as e:
            logger.error(f"RDS import failed: {e}")
            raise RuntimeError(f"RDS import failed: {e}")

# Main Interactive CLI
def main():
    """CLI entry point — choose import type and run importer."""
    logger.info("Starting Data Importer CLI...")

    import_type = input("Enter import type (S3 or RDS): ").strip().upper()

    if import_type not in ["S3", "RDS"]:
        logger.error("Invalid import type. Choose either 'S3' or 'RDS'.")
        sys.exit(1)

    try:
        importer = DataImporterFactory.get_importer(import_type)
        result = importer.import_data()
        logger.info(f"Import process completed successfully. Result: {result}")

    except Exception as e:
        logger.exception(f"Import process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
