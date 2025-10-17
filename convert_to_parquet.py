import configparser
from pathlib import Path
from datetime import datetime
import json
import pandas as pd


class ParequateConverter:
    """Converts Excel chunk files to Parquet format and generates a manifest."""
    def __init__(self, config_path: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            section = self.config['PARQUET_CONVERSION']
            self.output_dir = Path(section.get('output_dir', 'data/parquet_chunks'))
            self.output_prefix = section.get('output_prefix', 'converted_chunk_')

            self.excel_manifest_path = Path(section.get('excel_manifest', 'data/excel_chunks/excel_manifest_latest.json'))

        except Exception as e:
            raise ValueError(f"Configuration error: {e}")

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def convert_chunks_to_parquet(self, excel_files: list[Path]):
        """Converts a list of Excel files to Parquet format and creates a manifest."""
        if not excel_files:
            raise FileNotFoundError("No Excel files passed for conversion.")

        print(f"Converting {len(excel_files)} Excel files to Parquet.")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        manifest = []

        for idx, file in enumerate(excel_files, start=1):
            print(f"Converting file {idx}/{len(excel_files)}: {file.name}")
            try:
                df = pd.read_excel(file, engine="openpyxl")
                if df.empty:
                    print(f"Skipping empty file: {file.name}")
                    continue

                parquet_path = self.output_dir / f"{self.output_prefix}{idx}_{timestamp}.parquet"
                df.to_parquet(parquet_path, index=False, compression="snappy")

                file_size = parquet_path.stat().st_size / (1024 * 1024)
                manifest.append({
                    "chunk_id": idx,
                    "source_excel": file.name,
                    "parquet_file": parquet_path.name,
                    "rows": len(df),
                    "size_mb": round(file_size, 2),
                    "timestamp": timestamp
                })

                print(f"{file.name} => {parquet_path.name} ({file_size:.2f} MB)")

            except Exception as e:
                print(f"Conversion failed for {file.name}: {e}")

        manifest_file = self.output_dir / f"{self.output_prefix}manifest_{timestamp}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=4)

        print(f"\nParquet manifest created: {manifest_file}")
        return manifest_file


if __name__ == "__main__":
    try:
        print("Reading Excel manifest...")
        converter = ParequateConverter()

        if not converter.excel_manifest_path.exists():
            raise FileNotFoundError(f"Excel manifest not found at: {converter.excel_manifest_path}")

        with open(converter.excel_manifest_path, "r") as f:
            excel_manifest = json.load(f)

        # Build full paths to Excel files
        excel_dir = converter.excel_manifest_path.parent
        excel_files = [excel_dir / entry['filename'] for entry in excel_manifest]

        print("\nConverting Excel files to Parquet...")
        parquet_manifest = converter.convert_chunks_to_parquet(excel_files)

        print(f"\nConversion completed. Parquet manifest: {parquet_manifest.name}")

    except Exception as e:
        print(f"Error during conversion: {e}")
