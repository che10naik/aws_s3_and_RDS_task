from pathlib import Path
import json
from datetime import datetime
import configparser
import boto3

class S3Uploader:
    """Uploads Parquet files to AWS S3 based on a manifest file."""
    def __init__(self, config_path: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            aws = self.config['AWS']
            parquet = self.config['PARQUET_CONVERSION']

            self.bucket_name = aws.get('bucket_name')
            self.s3_prefix = aws.get('s3_prefix', '')
            self.region = aws.get('region', 'us-east-1')
        except Exception as e:
            raise ValueError(f"Configuration error in AWS section: {e}")

        self.output_dir = Path(parquet.get('output_dir', 'data/parquet_chunks'))
        self.manifest_dir = Path(aws.get('manifest_dir', 'data/manifests'))

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

        self.s3_client = boto3.client('s3', region_name=self.region)

    def get_latest_manifest(self) -> Path:
        """Retrieve the latest manifest file from the output directory."""
        manifests = sorted(self.output_dir.glob('*manifest_*.json'), key=lambda f: f.stat().st_mtime)
        if not manifests:
            raise FileNotFoundError("No manifest files found in the output directory.")
        return manifests[-1]

    def upload_file_from_manifest(self, manifest_path: Path):
        """uploading the files to S3 based on the manifest."""
        with open(manifest_path, "r") as f:
            manifest_data = json.load(f)

        upload_log = []
        for entry in manifest_data:
            file_path = self.output_dir / entry['parquet_file']
            if not file_path.exists():
                print(f"File not found, skipping: {file_path}")
                continue

            s3_key = f"{self.s3_prefix}/{file_path.name}"
            try:
                self.s3_client.upload_file(str(file_path), self.bucket_name, s3_key)
                print(f"Uploaded: {file_path.name} -> s3://{self.bucket_name}/{s3_key}")
                upload_log.append({
                    "file": file_path.name,
                    "s3_key": s3_key,
                    "rows": entry["rows"],
                    "size_mb": entry["size_mb"],
                    "uploaded_at": datetime.now().isoformat()
                })
            except Exception as e:
                print(f"Upload failed: {e}")

        # Upload manifest itself
        manifest_s3_key = f"{self.s3_prefix}/{manifest_path.name}"
        self.s3_client.upload_file(str(manifest_path), self.bucket_name, manifest_s3_key)
        print(f"\nManifest uploaded as: s3://{self.bucket_name}/{manifest_s3_key}")

        # Save upload log
        upload_manifest_path = self.manifest_dir / f"upload_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(upload_manifest_path, "w") as f:
            json.dump(upload_log, f, indent=4)

        print(f"Upload log saved: {upload_manifest_path}")
        return upload_manifest_path

if __name__ == "__main__":
    try:
        uploader = S3Uploader()
        latest_manifest = uploader.get_latest_manifest()
        print(f"Uploading files from manifest: {latest_manifest.name}")

        summary = uploader.upload_file_from_manifest(latest_manifest)
        print(f"Upload summary: {summary.name}")
    except Exception as e:
        print(f"Upload process failed: {e}")
