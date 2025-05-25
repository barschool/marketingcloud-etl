import os
import logging
import json
import requests
from datetime import datetime
from typing import Generator
import hashlib
import math

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, text, inspect
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class SalesforceExtractorBase:
    """Base class for extracting data from Salesforce Marketing Cloud API."""
    
    def __init__(self, custom_object_key: str, table_name: str) -> None:
        """Initialize the Salesforce extractor.
        
        Args:
            custom_object_key (str): The key of the custom object to extract.
            table_name (str): The name of the SQL table to store data in.
        """
        self.auth_endpoint = os.getenv("MKT_CLOUD_AUTH_ENDPOINT")
        self.data_endpoint = os.getenv("MKT_CLOUD_DATA_ENDPOINT")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.custom_object_key = custom_object_key
        self.table_name = table_name
        self.access_token = None
        self.rest_instance_url = None
        self.schema = os.getenv("SCHEMA", 'uat')
        self.engine = self._create_connection()
        self.metadata = MetaData()
        self._define_table()
    
    def _create_connection(self) -> Engine:
        """Create a connection to the SQL database.
        
        Returns:
            SQLAlchemy engine: Database connection.
        """
        return create_engine(
            f"mysql+pymysql://{os.getenv('USERNAME')}:{os.getenv('PASSWORD')}@"
            f"{os.getenv('HOST')}:{os.getenv('PORT')}/{self.schema}"
        )
    
    def _define_table(self) -> None:
        """Define the table schema for storing lead activity data."""
        self.table = Table(
            self.table_name,
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('hash', String(16), nullable=False, unique=True),
            Column('lead_id', String(255)),
            Column('url', String(1024)),
            Column('session_id', String(255)),
            Column('order', String(50)),
            Column('date', DateTime),
            Column('type_id', String(255)),
            Column('event_category', String(255)),
            Column('event_name', String(256)),
            schema=self.schema
        )
    
    def _ensure_table_exists(self) -> None:
        """Create the table if it doesn't exist."""
        with self.engine.begin() as conn:  # begin() ensures transaction handling
            inspector = inspect(conn)
            if not inspector.has_table(self.table_name, schema=self.schema):
                self.metadata.create_all(bind=conn)
                logger.info(f"Created table: {self.schema}.{self.table_name}")

    def _get_auth_token(self) -> None:
        """Get authentication token from Salesforce Marketing Cloud."""
        auth_url = f"{self.auth_endpoint}"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            response = requests.post(auth_url, json=payload)
            response.raise_for_status()
            auth_data = response.json()
            self.access_token = auth_data.get("access_token")
            self.rest_instance_url = auth_data.get("rest_instance_url")
            logger.info("Successfully authenticated with Salesforce Marketing Cloud")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            raise
    
    def _fetch_page(self, url: str) -> dict:
        """Fetch a page of data from the API.
        
        Args:
            url (str): The URL to fetch data from.
        
        Returns:
            dict: The API response.
        """
        if not self.access_token:
            self._get_auth_token()
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            # If token expired, try to refresh once
            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                self._get_auth_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return response.json()
            raise
    
    def _parse_date(self, date_str: str) -> datetime | None:
        """Parse date string from API to datetime object.
        
        Args:
            date_str (str): Date string in format "M/D/YYYY H:MM:SS AM/PM"
            
        Returns:
            datetime: Parsed datetime or None if parsing fails
        """
        try:
            return datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
       
    def _flatten_item(self, item: dict) -> dict:
        """Flatten nested keys and values into a single-level dictionary.
        
        Args:
            item (dict): Nested item with keys and values sections.
            
        Returns:
            dict: Flattened dictionary with properly formatted fields.
        """
        flattened = {
            "lead_id": item["keys"].get("lead_id", ""),
            "url": item["keys"].get("url", ""),
            "session_id": item["keys"].get("session_id", ""),
            "order": item["keys"].get("order", ""),
            "type_id": item["values"].get("type_id", ""),
            "event_category": item["values"].get("event_category", ""),

            # without query string and truncated to 256 chars
            "event_name": item["values"].get("event_name", "").split("?")[0][:256]  
        }
        
        # Parse date string to datetime
        date_str = item["values"].get("date", "")
        flattened["date"] = self._parse_date(date_str)
        
        # Add hash
        serialized = json.dumps(item, sort_keys=True, separators=(',', ':')).encode('utf-8')  
        flattened["hash"] = hashlib.blake2b(serialized, digest_size=8).hexdigest()  # 8-byte hash -> 16 hex chars
        return flattened
    
    def _get_record_count(self) -> int:
        """Get the count of records in the database table.
        
        Returns:
            int: Number of records in the table
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}"))
                return result.scalar() or 0
        except SQLAlchemyError as e:
            logger.warning(f"Error getting record count, assuming empty table: {e._message()}")
            return 0

    def _batch_insert(self, records: list[dict]) -> int:
        """Insert records in batches to avoid memory issues.
        
        Args:
            records (list): List of records to insert
            
        Returns:
            int: Number of records inserted
        """
        self._ensure_table_exists()
        
        if not records:
            return 0
        
        try:
            with self.engine.connect() as conn:
                # Use MySQL's `insert...on duplicate key update` to handle duplicates
                stmt = insert(self.table).values(records)
                on_duplicate_stmt = stmt.on_duplicate_key_update(
                    hash=stmt.inserted.hash  # No-op update to avoid duplicate insertion
                )
                _result = conn.execute(on_duplicate_stmt)
                conn.commit()
                
            inserted_count = len(records)
            logger.info(f"Attempted to insert {inserted_count} records (duplicates ignored by DB)")
            return inserted_count
        
        except SQLAlchemyError as e:
            logger.error(f"Error in batch insert: {e._message()}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during batch insert: {e}")
            raise
            


class SalesforceLeadActivity(SalesforceExtractorBase):
    """Class for extracting Lead Activity data from Salesforce Marketing Cloud API."""
    
    def __init__(self, mode: str = "bulk", ) -> None:
        """Initialize the Lead Activity extractor.
        
        Args:
            mode (str): Extraction mode ('bulk' or 'incremental').
        """
        super().__init__(
            custom_object_key="SW_Lead_Activity_7f2a6388",
            table_name="salesforce_lead_activity"
        )
        self.mode = mode
        self.page_size = 2500  # As per API response  
    
    def _get_page_generator(self, db_record_count: int = 0) -> Generator[dict, None, None] | None:
        """Generate API response pages one at a time for new records.
        
        Fetches only the latest pages that contain new records, starting from
        the appropriate page based on the difference between API and DB counts.
        
        Args:
            db_record_count (int): Total number of records in the database
            
        Yields:
            dict: API response data for each page containing new records
        """
        
        # Make first request to get total count
        start_page = max(1, math.ceil(db_record_count / self.page_size))
        first_page = self._fetch_page(f"{self.data_endpoint}?$page=1")
        
        # Get total record count from API
        api_record_count = first_page.get("count", 0)
        total_pages = math.ceil(api_record_count / self.page_size)

        if api_record_count > db_record_count:
            total_pages = math.ceil(api_record_count / self.page_size)
            logger.info(f"Starting from page {start_page} to get newest records, total records: {api_record_count}")
            
            # Fetch pages from start_page to total_pages to get newest records
            for page in range(start_page, total_pages + 1):
                logger.info(f"Fetching page {page} of {total_pages}")
                yield self._fetch_page(f"{self.data_endpoint}?$page={page}")
        else:
            logger.info("No new records to extract")

    def _process_page_items(self, response_data: dict) -> Generator[dict, None, None]:
        """Process items from a page and yield flattened records.
        
        Args:
            response_data (dict): API response data
            
        Yields:
            dict: Flattened record
        """
        items = response_data.get("items", [])
        for item in items:
            yield self._flatten_item(item)
    
    def _process_items_batch(self, pages_generator: Generator[dict, None, None] | None, 
                             batch_size: int = 2500) -> None:
        """Process items from pages generator and insert in batches.
        
        Args:
            pages_generator (Generator): Generator of API response pages
            batch_size (int): Size of batches for DB insertion
        """
        if pages_generator is None:
            logger.info("No pages to process")
            return
        records_batch = []
        total_processed = 0
        total_inserted = 0
        
        # Process each page
        for response_data in pages_generator:
            # Process each item in the page
            for record in self._process_page_items(response_data):
                total_processed += 1
                records_batch.append(record)
                
                # Insert batch when it reaches batch_size
                if len(records_batch) >= batch_size:
                    inserted = self._batch_insert(records_batch)
                    total_inserted += inserted
                    records_batch = []  # Clear batch after insertion
            
            logger.info(f"Processed {total_processed} records, inserted {total_inserted}")
        
        # Insert any remaining records
        if records_batch:
            inserted = self._batch_insert(records_batch)
            total_inserted += inserted
            
        logger.info(f"Completed processing: {total_processed} records processed, {total_inserted} inserted")
    
    def bulk_extract(self) -> None:
        """Extract all records in bulk mode and save to database.
        
        Uses a generator-based approach to minimize memory usage.
        """
        logger.info("Starting bulk extraction")
        self._get_auth_token()
        self._ensure_table_exists()
        
        # Make first request to get total count
        logger.info("Fetching first page to determine total records")
        first_page = self._fetch_page(f"{self.data_endpoint}?$page=1")
        
        total_count = first_page.get("count", 0)
        total_pages = (total_count // self.page_size) + (1 if total_count % self.page_size > 0 else 0)
        logger.info(f"Total records: {total_count}, Total pages: {total_pages}")
        
        if total_count == 0:
            logger.warning("No records found for bulk extraction")
            return
            
        # Clear existing table data for bulk mode
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {self.schema}.{self.table_name}"))
                conn.commit()
                logger.info(f"Truncated table: {self.schema}.{self.table_name}")
        except SQLAlchemyError as e:
            logger.warning(f"Error truncating table (it may not exist yet): {e._message()}")
        
        # Create page generator 
        pages_generator = self._get_page_generator()
        
        # Process all pages
        self._process_items_batch(
            pages_generator,  # Include first page
            batch_size=2500
        )
        
        logger.info("Bulk extraction completed")
    
    def incremental_extract(self) -> None:
        """Extract only new records in incremental mode and save to database.
        
        Uses a generator-based approach to minimize memory usage.
        """
        logger.info("Starting incremental extraction")
        self._get_auth_token()
        self._ensure_table_exists()
        
        # Get record count from database
        db_record_count = self._get_record_count()
        logger.info(f"Current database record count: {db_record_count}")
        
        # Only fetch the needed pages
        pages_generator = self._get_page_generator(db_record_count)
        
        # Process pages without filtering based on existing hashes
        self._process_items_batch(
            pages_generator,  # Include first page
            batch_size=2500
        )
        
        logger.info("Incremental extraction completed")