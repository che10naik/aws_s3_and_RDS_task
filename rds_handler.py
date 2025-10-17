import configparser
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Base for ORM models
Base = declarative_base()

# Logger setup
def get_logger():
    """Set up and return a logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


logger = get_logger()


# Dynamically generated Employee model
def create_employee_model(table_name: str):
    """Create an Employee model for the given table name."""
    class Employee(Base):
        """Creating table and model dynamically."""
        __tablename__ = table_name

        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(255), nullable=False)
        salary = Column(Float, nullable=False)
        salary_date = Column(Date, nullable=False)

        def to_dict(self):
            """Convert ORM object to dictionary."""
            return {
                "id": self.id,
                "name": self.name,
                "salary": self.salary,
                "salary_date": self.salary_date.isoformat(),
            }

    return Employee


class RDSTableHandler:
    """Handles RDS table creation, insertion, and CRUD operations."""

    def __init__(self, config_path: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            rds_cfg = self.config["RDS"]
            dialect = rds_cfg.get("dialect", "mysql")
            driver = rds_cfg.get("driver", "pymysql")
            username = rds_cfg.get("username")
            password = rds_cfg.get("password")
            host = rds_cfg.get("host")
            port = rds_cfg.get("port", "3306")
            database = rds_cfg.get("database")
            self.table_name = rds_cfg.get("table_name", "employees")

            if not all([username, password, host, database]):
                raise ValueError("Missing required RDS connection parameters.")

            # Build SQLAlchemy connection string
            self.connection_url = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"

        except KeyError as e:
            raise ValueError(f"Missing RDS config section or key: {e}")

        # Create engine and session
        self.engine = create_engine(self.connection_url, pool_pre_ping=True, future=True)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

        # Create dynamic Employee model
        self.Employee = create_employee_model(self.table_name)

        # Verify connection immediately
        try:
            with self.engine.connect() as conn:
                logger.info(f"Connected successfully to RDS: {conn.engine.url}")
        except Exception as e:
            logger.error(f"Failed to connect to RDS: {e}")
            raise

    # Table Creation
    def create_table_if_not_exists(self):
        """Creates the employee table if it does not exist."""
        Base.metadata.create_all(self.engine)
        logger.info(f"Table '{self.table_name}' verified or created successfully.")

    # Count / Bulk Insert
    def count_rows(self) -> int:
        """Count rows in the employee table."""
        with self.SessionLocal() as session:
            count = session.query(self.Employee).count()
            logger.info(f"Row count in '{self.table_name}': {count}")
            return count

    def insert_many(self, rows):
        """Insert multiple employee records."""
        if not rows:
            logger.warning("No rows provided for insertion.")
            return 0
        with self.SessionLocal() as session:
            try:
                session.bulk_insert_mappings(self.Employee, rows)
                session.commit()
                logger.info(f"Inserted {len(rows)} records into '{self.table_name}'.")
                return len(rows)
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Insert failed: {e}")
                raise

    # CRUD Operations
    def get_all(self):
        """Retrieve all employee records."""
        with self.SessionLocal() as session:
            rows = session.query(self.Employee).all()
            return [r.to_dict() for r in rows]

    def get_by_id(self, emp_id: int):
        """Retrieve a single employee by ID."""
        with self.SessionLocal() as session:
            row = session.get(self.Employee, emp_id)
            return row.to_dict() if row else None

    def create(self, payload: dict):
        """Create a new employee record."""
        with self.SessionLocal() as session:
            emp = self.Employee(**payload)
            session.add(emp)
            session.commit()
            session.refresh(emp)
            logger.info(f"Employee created: {emp.id}")
            return emp.to_dict()

    def update(self, emp_id: int, payload: dict):
        """Update an existing employee record."""
        with self.SessionLocal() as session:
            emp = session.get(self.Employee, emp_id)
            if not emp:
                return None
            for key, value in payload.items():
                setattr(emp, key, value)
            session.commit()
            session.refresh(emp)
            logger.info(f"Employee updated: {emp.id}")
            return emp.to_dict()

    def delete(self, emp_id: int) -> bool:
        """Delete an employee record."""
        with self.SessionLocal() as session:
            emp = session.get(self.Employee, emp_id)
            if not emp:
                return False
            session.delete(emp)
            session.commit()
            logger.info(f"Employee deleted: {emp_id}")
            return True


if __name__ == "__main__":
    handler = RDSTableHandler()
    handler.create_table_if_not_exists()
    print("Connected and verified table.")
    print(f"Total rows: {handler.count_rows()}")
