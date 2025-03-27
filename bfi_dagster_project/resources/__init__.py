import os
import time
import sqlite3
import datetime
import functools
import dagster as dg
from multiprocessing import Pool
from contextlib import contextmanager


class ProcessPoolResource:
    def __init__(self, num_proc=2):
        self.num_proc = num_proc
        #self,_pool = None

    @contextmanager
    def get_pool(self):
        pool = Pool(processes=self.num_proc)
        try:
            yield pool
        finally:
            pool.close()
            pool.join()

    def map(self, func, iterable):
        with self.get_pool() as pool:
            return pool.map(func, iterable)


@dg.resource
def process_pool(init_context):
    return ProcessPoolResource(num_proc=init_context.resource_config.get("num_processes", 2))


# Load ENV for database
DATABASE = os.environ.get('DATABASE')


def with_retries(max_retries=5, retry_delay=1.0, backoff_factor=2.0,
                 retryable_exceptions=(sqlite3.OperationalError,)):
    """
    Decorator that implements retry logic with exponential backoff
    Args:
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for delay on each subsequent retry
        retryable_exceptions: Tuple of exceptions that should trigger a retry
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, context, *args, **kwargs):
            last_exception = None
            current_delay = retry_delay
            for attempt in range(max_retries + 1):
                try:
                    return func(self, context, *args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    # Check if this is a retryable error message
                    error_msg = str(e).lower()
                    if "database is locked" in error_msg or "unable to open database file" in error_msg:
                        if attempt < max_retries:
                            # Log retry attempt
                            context.log.warning(
                                f"SQLite operation failed (attempt {attempt+1}/{max_retries+1}): {e}. "
                                f"Retrying in {current_delay:.2f}s..."
                            )
                            time.sleep(current_delay)
                            current_delay *= backoff_factor
                        else:
                            context.log.error(f"Maximum retries reached. Last error: {e}")
                            raise
                    else:
                        # Not a retryable error, re-raise immediately
                        context.log.error(f"Non-retryable SQLite error: {e}")
                        raise

            # This should never be reached, but just in case
            raise last_exception
        return wrapper
    return decorator


class SQLiteResource(dg.ConfigurableResource):
    filepath: str = DATABASE
    max_retries: int = 5
    retry_delay: float = 1.0
    timeout: float = 300.0

    @contextmanager
    def get_connection(
        self,
        context: dg.AssetExecutionContext
    ):
        """
        Context manager for database connections to ensure proper closure
        """
        attempt = 0
        current_delay = self.retry_delay
        while True:
            try:
                conn = sqlite3.connect(self.filepath, timeout=self.timeout)
                context.log.info("Connected to database: %s", self.filepath)

                # Set pragmas for better concurrency
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(f"PRAGMA busy_timeout={int(self.timeout * 1000)}")

                try:
                    yield conn
                finally:
                    # Ensure we always commit and close properly
                    try:
                        conn.commit()
                        # Force WAL checkpoint before closing
                        conn.execute("PRAGMA wal_checkpoint(FULL)")
                    except sqlite3.Error as e:
                        context.log.error("Error during connection cleanup: %s", e)
                    finally:
                        conn.close()
                        context.log.info("Disconnected from database: %s", self.filepath)

                # If we get here, everything worked so we break the retry loop
                break

            except sqlite3.OperationalError as e:
                attempt += 1
                if attempt > self.max_retries:
                    context.log.error(f"Failed to connect to database after {self.max_retries} attempts: {e}")
                    raise

                context.log.warning(
                    f"Failed to connect to database (attempt {attempt}/{self.max_retries}): {e}. "
                    f"Retrying in {current_delay:.2f}s..."
                )
                time.sleep(current_delay)
                current_delay *= 2  # Exponential backoff

    @with_retries()
    def initialise_db(
        self,
        context: dg.AssetExecutionContext
    ):
        """
        Initialize and maintain connection to SQLite database tracking encoding progress.
        """
        context.log.info("Initialising database")
        with self.get_connection(context) as conn:
            cursor = conn.cursor()

            # Enable WAL mode for better concurrent access / busy time out / create table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS encoding_status (
                    process_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq_id TEXT NOT NULL,
                    status TEXT DEFAULT 'Started',
                    folder_path TEXT NOT NULL,
                    first_image TEXT,
                    last_image TEXT,
                    gaps_in_sequence TEXT,
                    assessment_pass TEXT,
                    assessment_complete TIMESTAMP,
                    colourspace TEXT,
                    seq_size INTEGER,
                    bitdepth INTEGER,
                    image_width TEXT,
                    image_height TEXT,
                    process_start TIMESTAMP,
                    encoding_choice TEXT,
                    encoding_log TEXT,
                    encoding_retry TEXT,
                    encoding_complete TIMESTAMP,
                    derivative_path TEXT,
                    derivative_size INTEGER,
                    derivative_md5 TEXT,
                    validation_complete TIMESTAMP,
                    validation_success TEXT,
                    error_message TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sequence_deleted TEXT,
                    moved_to_autoingest TEXT
                )
            """)
            context.log.info("Database initialized at: %s", self.filepath)

    @with_retries()
    def start_process(self, context: dg.AssetExecutionContext, seq_id: str, folder_path: str, status: str) -> list[tuple]:
        """
        Initializes a new process record with proper connection handling
        """
        with self.get_connection(context) as conn:
            cur = conn.cursor()
            context.log.info("Creating new ROW with cursor:")
            timestamp = str(datetime.datetime.today())[:19]

            # Use parameterized query to prevent SQL injection
            query = """
            INSERT INTO encoding_status
            (seq_id, folder_path, status, process_start, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """
            cur.execute(query, (seq_id, folder_path, status, timestamp, timestamp))

        # Return after retrieval to ensure we see the inserted data
        return self.retrieve_seq_id_row(context, "SELECT * FROM encoding_status WHERE seq_id=?", 'fetchone', (seq_id,))

    @with_retries()
    def append_to_database(
        self,
        context: dg.AssetExecutionContext,
        seq_id: str,
        arguments: str
    ):
        """
        Using seq_id field, indentify row and update with supplied argument string
        """
        # Prepare column names and placeholders
        column_names = [arg_pair[0] for arg_pair in arguments] + ["last_updated"]
        placeholders = ", ".join(["?" for _ in range(len(column_names))])
        set_clause = ", ".join([f"{col} = ?" for col in column_names])

        # Prepare values
        values = [arg_pair[1] for arg_pair in arguments]
        values.append(str(datetime.datetime.today())[:19])  # Add current timestamp

        # Build the final parameter list with seq_id at the end
        all_params = values + [seq_id]

        with self.get_connection(context) as conn:
            cursor = conn.cursor()

            # Use parameterized query
            query = f"UPDATE encoding_status SET {set_clause} WHERE seq_id = ?"
            context.log.info(f"Executing update query: {query}")
            cursor.execute(query, all_params)

        # Return after retrieval to ensure we see the updated data
        return self.retrieve_seq_id_row(context, "SELECT * FROM encoding_status WHERE seq_id = ?", 'fetchone', (seq_id,))

    @with_retries()
    def retrieve_seq_id_row(self, context: dg.AssetExecutionContext, query, fetch_arg, params=()):
        """
        Access database and retrieve requested entries
        """
        context.log.info("Retrieve seq id row() start")
        with self.get_connection(context) as conn:
            cur = conn.cursor()
            cur.execute(query, params)

            if fetch_arg == 'fetchone':
                return cur.fetchone()
            return cur.fetchall()

    @with_retries()
    def get_all_records(self, context: dg.AssetExecutionContext,):
        """
        Retrieve all records from the database
        """
        with self.get_connection(context) as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM encoding_status")
            return cur.fetchall()

    @with_retries()
    def diagnose_database(self, context: dg.AssetExecutionContext):
        """
        Diagnostic function to check database state
        """
        context.log.info("Running database diagnostics on: %s", self.filepath)

        # Check file existence and size
        file_exists = os.path.exists(self.filepath)
        file_size = os.path.getsize(self.filepath) if file_exists else 0
        wal_file = f"{self.filepath}-wal"
        wal_exists = os.path.exists(wal_file)
        wal_size = os.path.getsize(wal_file) if wal_exists else 0

        context.log.info("Database file exists: %s, size: %s bytes", file_exists, file_size)
        context.log.info("WAL file exists: %s, size: %s bytes", wal_exists, wal_size)

        if not file_exists:
            context.log.error("Database file doesn't exist!")
            return

        with self.get_connection(context) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            context.log.info("Tables in database: %s", tables)

            if any('encoding_status' in str(table) for table in tables):
                cursor.execute("SELECT COUNT(*) FROM encoding_status")
                count = cursor.fetchone()[0]
                context.log.info("Rows in encoding_status: %s", count)

                if count > 0:
                    cursor.execute("SELECT * FROM encoding_status LIMIT 1")
                    context.log.info("Sample row: %s", cursor.fetchone())
            else:
                context.log.warning("encoding_status table not found in database!")
