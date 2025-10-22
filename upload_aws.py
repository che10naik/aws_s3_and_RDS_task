from pathlib import Path
import json
from datetime import datetime
import configparser
import boto3
import logging


# Logger Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# S3Uploader Class
class S3Uploader:
    """Uploads Parquet files to AWS S3 based on a manifest file."""
    
    def __init__(self, config_path: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            aws = self.config["AWS"]
            parquet = self.config["PARQUET_CONVERSION"]

            self.bucket_name = aws.get("bucket_name")
            self.region = aws.get("region", "us-east-1")
            self.s3_prefix = aws.get("s3_prefix", "").strip()

            if not self.s3_prefix or self.s3_prefix == "/":
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.s3_prefix = f"uploads_{timestamp}"
                logger.warning(f"Using auto-generated S3 folder name: {self.s3_prefix}")

        except Exception as e:
            raise ValueError(f"Configuration error in AWS section: {e}")

        self.output_dir = Path(parquet.get("output_dir", "data/parquet_chunks"))
        self.manifest_dir = Path(aws.get("manifest_dir", "data/manifests"))

        # Ensure local directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

        # Create S3 client
        self.s3_client = boto3.client("s3", region_name=self.region)

        logger.info(f"S3Uploader initialized for bucket '{self.bucket_name}' in region '{self.region}'.")

    # Get Latest Manifest
    def get_latest_manifest(self) -> Path:
        """Retrieve the latest manifest file from the output directory."""
        manifests = sorted(self.output_dir.glob("*manifest_*.json"), key=lambda f: f.stat().st_mtime)
        if not manifests:
            raise FileNotFoundError("No manifest files found in the output directory.")
        latest_manifest = manifests[-1]
        logger.info(f"Latest manifest detected: {latest_manifest.name}")
        return latest_manifest

    # Upload Files from Manifest
    def upload_file_from_manifest(self, manifest_path: Path):
        """Upload the files to S3 based on the manifest."""
        with open(manifest_path, "r") as f:
            manifest_data = json.load(f)

        upload_log = []

        for entry in manifest_data:
            file_path = self.output_dir / entry["parquet_file"]
            if not file_path.exists():
                logger.warning(f"File not found, skipping: {file_path}")
                continue

            s3_key = f"{self.s3_prefix}/{file_path.name}".lstrip("/")
            try:
                self.s3_client.upload_file(str(file_path), self.bucket_name, s3_key)
                logger.info(f"Uploaded: {file_path.name} → s3://{self.bucket_name}/{s3_key}")

                upload_log.append({
                    "file": file_path.name,
                    "s3_key": s3_key,
                    "rows": entry["rows"],
                    "size_mb": entry["size_mb"],
                    "uploaded_at": datetime.now().isoformat()
                })

            except Exception as e:
                logger.error(f"Upload failed for {file_path.name}: {e}")

        # Upload Manifest File
        manifest_s3_key = f"{self.s3_prefix}/{manifest_path.name}".lstrip("/")
        try:
            self.s3_client.upload_file(str(manifest_path), self.bucket_name, manifest_s3_key)
            logger.info(f"Manifest uploaded as: s3://{self.bucket_name}/{manifest_s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload manifest: {e}")

        # Save Upload Log Locally
        upload_manifest_path = self.manifest_dir / f"upload_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(upload_manifest_path, "w") as f:
            json.dump(upload_log, f, indent=4)

        logger.info(f"Upload log saved locally at: {upload_manifest_path}")
        return upload_manifest_path


if __name__ == "__main__":
    try:
        uploader = S3Uploader()
        latest_manifest = uploader.get_latest_manifest()
        logger.info(f"Uploading files from manifest: {latest_manifest.name}")

        summary = uploader.upload_file_from_manifest(latest_manifest)
        logger.info(f"Upload summary saved at: {summary}")

    except Exception as e:
        logger.error(f"Upload process failed: {e}")
