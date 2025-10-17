import sys
from upload_aws import S3Uploader
from data_importer import LocalParquetImporter

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
            print(f"Uploading files from manifest: {latest_manifest.name}")
            summary = uploader.upload_file_from_manifest(latest_manifest)
            print(f"Upload summary saved at: {summary}")
            return summary
        except Exception as e:
            raise RuntimeError(f"S3 import failed: {e}")

class RDSImporterWrapper:
    """Wrapper around LocalParquetImporter to match import_data interface."""
    def import_data(self):
        """Main method to import Parquet files into RDS table."""
        try:
            importer = LocalParquetImporter(row_limit=10000)
            importer.import_to_rds()
            return f"RDS import completed. Table: {importer.handler.table_name}"
        except Exception as e:
            raise RuntimeError(f"RDS import failed: {e}")


# Main Interactive CLI
def main():
    """ Ask user which import type to use """
    import_type = input("Enter import type (S3 or RDS): ").strip().upper()

    if import_type not in ["S3", "RDS"]:
        print("Invalid import type. Choose either 'S3' or 'RDS'.")
        sys.exit(1)

    try:
        importer = DataImporterFactory.get_importer(import_type)
        result = importer.import_data()
        print("Result:", result)

    except Exception as e:
        print("Error:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
