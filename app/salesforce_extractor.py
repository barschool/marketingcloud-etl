import os
import base64
import logging
import requests
from datetime import datetime
from typing import Generator

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, text
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
            f"{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('SCHEMA')}"
        )
    
    def _define_table(self) -> None:
        """Define the table schema for storing lead activity data."""
        self.table = Table(
            self.table_name,
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('hash', String(255), unique=True, nullable=False),
            Column('lead_id', String(255)),
            Column('url', String(1024)),
            Column('session_id', String(255)),
            Column('order', String(50)),
            Column('date', DateTime),
            Column('type_id', String(255)),
            Column('event_category', String(255)),
            Column('event_name', String(255)),
            schema='uat'
        )
    
    def _ensure_table_exists(self) -> None:
        """Create the table if it doesn't exist."""
        with self.engine.connect() as conn:
            if not self.engine.dialect.has_table(conn, self.table_name, schema='uat'):
                self.metadata.create_all(self.engine)
                logger.info(f"Created table: uat.{self.table_name}")
    
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
    
    def _generate_hash(self, item: dict) -> str:
        """Generate a unique hash for a record based on all fields.
        
        Args:
            item (dict): Item data with keys and values.
            
        Returns:
            str: Base64 encoded hash.
        """
        # Combine all key and value fields
        combined = ""
        for key_name, key_value in item["keys"].items():
            combined += str(key_value or "")
            
        for value_name, value_value in item["values"].items():
            combined += str(value_value or "")
        
        # Encode to bytes and then to base64
        combined_bytes = combined.encode('utf-8')
        return base64.b64encode(combined_bytes).decode('utf-8')
    
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
            "event_name": item["values"].get("event_name", "")
        }
        
        # Parse date string to datetime
        date_str = item["values"].get("date", "")
        flattened["date"] = self._parse_date(date_str)
        
        # Add hash
        flattened["hash"] = self._generate_hash(item)
        
        return flattened
    
    def _get_record_count(self) -> int:
        """Get the count of records in the database table.
        
        Returns:
            int: Number of records in the table
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM uat.{self.table_name}"))
                return result.scalar() or 0
        except SQLAlchemyError as e:
            logger.warning(f"Error getting record count, assuming empty table: {e}")
            return 0
    
    def _get_existing_hashes(self) -> set:
        """Get set of existing hashes from the database.
        
        Returns:
            set: Set of hash values already in the database
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT hash FROM uat.{self.table_name}"))
                return {row[0] for row in result}
        except SQLAlchemyError as e:
            logger.warning(f"Error getting existing hashes, assuming empty table: {e}")
            return set()
    
    def _batch_insert(self, records: list[dict]) -> int:
        """Insert records in batches to avoid memory issues.
        
        Args:
            records (list): List of records to insert
            batch_size (int): Batch size for insertion - only used for fallback
            
        Returns:
            int: Number of records inserted
        """
        self._ensure_table_exists()
        
        if not records:
            return 0
        
        try:
            # Direct bulk insert without re-batching
            with self.engine.connect() as conn:
                result = conn.execute(
                    insert(self.table).values(records)
                )
                conn.commit()
                
            inserted_count = len(records)
            logger.info(f"Bulk inserted {inserted_count} records in one operation")
            return inserted_count
        
        except SQLAlchemyError as e:
            logger.error(f"Error in bulk insert: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during bulk insert: {e}")
            raise 
            
            


class SalesforceLeadActivity(SalesforceExtractorBase):
    """Class for extracting Lead Activity data from Salesforce Marketing Cloud API."""
    
    def __init__(self, mode: str = "bulk") -> None:
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
    
    def _get_bulk_page_generator(self) -> Generator[dict, None, None]:
        """Generate API response pages one at a time.
        
        Args:
            total_pages (int): Total number of pages to fetch
            base_url (str): Base URL for API requests
            
        Yields:
            dict: API response data for each page
        """
        base_url = f"{self.data_endpoint}"
        
        # Make first request to get total count
        logger.info("Fetching first page to determine total records")
        first_page = self._fetch_page(f"{base_url}?$page=1")
        
        total_count = first_page.get("count", 0)
        total_pages = (total_count // self.page_size) + (1 if total_count % self.page_size > 0 else 0)
        logger.info(f"Total records: {total_count}, Total pages: {total_pages}")

        for page in range(1, total_pages + 1):
            logger.info(f"Fetching page {page} of {total_pages}")
            url = f"{base_url}?$page={page}"
            yield self._fetch_page(url)
    
    
    def _get_incremental_page_generator(self, db_record_count: int) -> Generator[dict, None, None] | None:
        """Generate API response pages one at a time for new records.
        
        Fetches only the latest pages that contain new records, starting from
        the appropriate page based on the difference between API and DB counts.
        
        Args:
            db_record_count (int): Total number of records in the database
            
        Yields:
            dict: API response data for each page containing new records
        """
        base_url = f"{self.data_endpoint}"
        
        # Make first request to get total count
        logger.info("Fetching first page to determine total records")
        first_page = self._fetch_page(f"{base_url}?$page=1")
        
        # Get total record count from API
        api_record_count = first_page.get("count", 0)
        total_pages = (api_record_count // self.page_size) + (1 if api_record_count % self.page_size > 0 else 0)
        logger.info(f"API record count: {api_record_count}, Total pages: {total_pages}")
        
        # Calculate difference and pages needed
        records_difference = max(0, api_record_count - db_record_count)
        pages_needed = (records_difference // self.page_size) + (1 if records_difference % self.page_size > 0 else 0)
        
        if records_difference <= 0:
            logger.info("No new records to extract")
            return None
        
        # Calculate the starting page (newest records are in the latest pages)
        start_page = max(1, total_pages - pages_needed + 1)
        
        logger.info(f"Records difference: {records_difference}, Pages needed: {pages_needed}")
        logger.info(f"Starting from page {start_page} to get newest records")
        
        # Fetch pages from start_page to total_pages to get newest records
        for page in range(start_page, total_pages + 1):
            logger.info(f"Fetching page {page} of {total_pages}")
            url = f"{base_url}?$page={page}"
            yield self._fetch_page(url)
    
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
                             existing_hashes: set | None = None, 
                             batch_size: int = 2500) -> None:
        """Process items from pages generator and insert in batches.
        
        Args:
            pages_generator (Generator): Generator of API response pages
            existing_hashes (set, optional): Set of hashes to filter against
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
                
                # Skip if hash already exists in database (for incremental mode)
                if existing_hashes is not None and record["hash"] in existing_hashes:
                    continue
                    
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
        
        base_url = f"{self.data_endpoint}"
        
        # Make first request to get total count
        logger.info("Fetching first page to determine total records")
        first_page = self._fetch_page(f"{base_url}?$page=1")
        
        total_count = first_page.get("count", 0)
        total_pages = (total_count // self.page_size) + (1 if total_count % self.page_size > 0 else 0)
        logger.info(f"Total records: {total_count}, Total pages: {total_pages}")
        
        if total_count == 0:
            logger.warning("No records found for bulk extraction")
            return
            
        # Clear existing table data for bulk mode
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE uat.{self.table_name}"))
                conn.commit()
                logger.info(f"Truncated table: uat.{self.table_name}")
        except SQLAlchemyError as e:
            logger.warning(f"Error truncating table (it may not exist yet): {e}")
        
        # Create page generator 
        pages_generator = self._get_bulk_page_generator()
        
        # Process all pages
        self._process_items_batch(
            pages_generator,  # Include first page
            existing_hashes=None,  # No filtering in bulk mode
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
        
        # Get existing hashes and record count from database
        existing_hashes = self._get_existing_hashes()
        db_record_count = self._get_record_count()
        logger.info(f"Current database record count: {db_record_count}")
        
        # Only fetch the needed pages
        pages_generator = self._get_incremental_page_generator(db_record_count)
        
        # Process pages with filtering based on existing hashes
        self._process_items_batch(
            pages_generator,  # Include first page
            existing_hashes=existing_hashes,  # Filter against existing hashes
            batch_size=2500
        )
        
        logger.info("Incremental extraction completed")