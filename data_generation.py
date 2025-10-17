from datetime import datetime, timedelta, date
from dataclasses import dataclass, asdict
from pathlib import Path
import multiprocessing as mp
import json
import configparser
import numpy as np
import pandas as pd
from faker import Faker


@dataclass
class Employees:
    """Dataclass for employee records."""
    empid: int
    name: str
    salary: float
    salary_date: datetime

def _generate_chunk(start_id, num_rows, salary_min, salary_max, days_back):
    fake = Faker()
    start_date = date.today() - timedelta(days=days_back)
    end_date = date.today()
    employee = []

    for i in range(num_rows):
        empid = start_id + i
        name = fake.name()
        salary = round(np.random.uniform(salary_min, salary_max), 2)
        salary_date = fake.date_between_dates(date_start=start_date, date_end=end_date)
        employee.append(Employees(empid, name, salary, salary_date))

    return pd.DataFrame([asdict(e) for e in employee])

class DataGenerator:
    """Generates synthetic employee data and saves to Excel files in chunks."""
    def __init__(self, config_path: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            section = self.config['DATA_GENERATION']
            self.NUM = section.getint('num_rows')
            self.SALARY_MIN = section.getfloat('salary_min')
            self.SALARY_MAX = section.getfloat('salary_max')
            self.DAYS_BACK = section.getint('days_back')
            self.output_excel = Path(section.get('output_excel'))
            self.output_excel.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Configuration error in DATA_GENERATION section: {e}")

        self.fake = Faker()

    def generate_data_parallel(self) -> dict:
        """Generates data in parallel using multiprocessing and saves in chunked Excel files."""
        total_rows = self.NUM
        num_cores = mp.cpu_count()

        base_rows = total_rows // num_cores
        remainder = total_rows % num_cores

        chunks = []
        start_id = 1

        for i in range(num_cores):
            rows_in_chunk = base_rows + (1 if i < remainder else 0)
            chunks.append((start_id, rows_in_chunk, self.SALARY_MIN, self.SALARY_MAX, self.DAYS_BACK))
            start_id += rows_in_chunk

        print(f"Using {num_cores} CPU cores for parallel generation.")
        with mp.Pool(processes=num_cores) as pool:
            results = pool.starmap(_generate_chunk, chunks)

        # Save each chunk separately with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        manifest = []
        excel_files = []

        for idx, df_chunk in enumerate(results, start=1):
            chunk_file = self.output_excel.with_stem(f"{self.output_excel.stem}_chunk_{idx}_{timestamp}")
            df_chunk.to_excel(chunk_file, index=False)
            excel_files.append(chunk_file)

            file_size = chunk_file.stat().st_size / (1024 * 1024)
            manifest.append({
                "chunk_id": idx,
                "filename": chunk_file.name,
                "rows": len(df_chunk),
                "size_mb": round(file_size, 2),
                "timestamp": timestamp
            })
            print(f"Saved chunk {idx} -> {chunk_file.name} ({len(df_chunk)} rows, {file_size:.2f} MB)")

        manifest_file = self.output_excel.parent / f"{self.output_excel.stem}_manifest_{timestamp}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=4)

        with open(f"data/excel_chunks/excel_manifest_latest.json", "w") as f:
            json.dump(manifest, f, indent=4)


        print(f"\nManifest saved as '{manifest_file.name}' with {len(manifest)} entries.")
        return {
            "manifest": manifest_file,
            "excel_files": excel_files  # returning newly created files
        }

if __name__ == "__main__":
    try:
        generator = DataGenerator()
        print("Generating data in parallel...")
        start_time = datetime.now()

        result = generator.generate_data_parallel()

        end_time = datetime.now()
        print(f"Data generation completed in {end_time - start_time}")
        print(f"Excel files created: {[f.name for f in result['excel_files']]}")
        print(f"Manifest: {result['manifest'].name}")

    except Exception as e:
        print(f"Error generating Excel file: {e}")
