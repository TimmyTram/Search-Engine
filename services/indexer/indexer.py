from database.db import DatabaseController
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from collections import Counter
import re
import datetime
import signal
from requests.exceptions import TooManyRedirects, RequestException

class ResumableIndexer:
    def __init__(self, db, table, timeout=5, insert_buffer_limit=100):
        self.db = db
        self.table = table
        self.timeout = timeout
        self.shutdown_requested = False

        self.insert_buffer = []
        self.insert_buffer_limit = insert_buffer_limit

        self.session = self._create_session()
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """Handle Ctrl+C and other interruption signals"""
        print("\n[INFO] Shutdown requested. Finishing current batch and saving progress...")
        self.shutdown_requested = True
        # Don't exit immediately, let the code finish the current batch

    def _create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        })

        return session

    def extract_text(self, url):
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup.get_text()
        except TooManyRedirects:
            print(f"[Redirect Error] Too many redirects for URL: {url}")
            return ""
        except RequestException as e:
            print(f"[Request Error] Failed to fetch {url}: {e}")
            return ""
    
    def extract_keywords(self, text):
        words = re.findall(r'\b\w+\b', text.lower())
        words = [word for word in words if word not in ENGLISH_STOP_WORDS and len(word) > 2]
        return Counter(words).most_common(10)

    def insert_keywords(self, keywords, page_id):
        for word, freq in keywords:
            self.insert_buffer.append({
                "keyword": word,
                "page_id": page_id,
                "frequency": freq
            })
            
        if len(self.insert_buffer) >= self.insert_buffer_limit:
            self.db.insert_many("inverted_index", self.insert_buffer)
            self.insert_buffer = []
    
    def update_index_status(self, page_id, status="indexed", error=None):
        """Update the indexing status of a URL in the database"""
        try:
            cursor = self.db.connection.cursor()
            
            if error:
                sql = """UPDATE indexing_status 
                        SET status = %s, last_indexed = %s, error = %s 
                        WHERE page_id = %s"""
                cursor.execute(sql, (
                    status, 
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    error[:255],  # Limit error message length
                    page_id
                ))
            else:
                sql = """UPDATE indexing_status 
                        SET status = %s, last_indexed = %s 
                        WHERE page_id = %s"""
                cursor.execute(sql, (
                    status, 
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    page_id
                ))
                
            self.db.connection.commit()
            cursor.close()
        except Exception as e:
            print(f"[DB Error] Failed to update index status: {e}")
    
    def get_indexing_status(self, page_id):
        """Get the current indexing status of a URL"""
        try:
            cursor = self.db.connection.cursor(dictionary=True)
            cursor.execute("SELECT status FROM indexing_status WHERE page_id = %s", (page_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result['status']
            return None
        except Exception as e:
            print(f"[DB Error] Failed to get index status: {e}")
            return None
    
    def create_or_update_index_status(self, page_id):
        """Create or update the indexing status record"""
        try:
            cursor = self.db.connection.cursor(dictionary=True)
            
            # Check if record exists
            cursor.execute("SELECT id FROM indexing_status WHERE page_id = %s", (page_id,))
            result = cursor.fetchone()
            
            if not result:
                # Create new record
                sql = """INSERT INTO indexing_status 
                        (page_id, status, last_indexed) 
                        VALUES (%s, %s, %s)"""
                cursor.execute(sql, (
                    page_id, 
                    "pending", 
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                self.db.connection.commit()
            
            cursor.close()
            return True
        except Exception as e:
            print(f"[DB Error] Failed to create/update index status: {e}")
            return False

    def clear_existing_index(self, page_id):
        """Remove existing index entries for a page before reindexing"""
        try:
            cursor = self.db.connection.cursor()
            cursor.execute("DELETE FROM inverted_index WHERE page_id = %s", (page_id,))
            self.db.connection.commit()
            cursor.close()
        except Exception as e:
            print(f"[DB Error] Failed to clear existing index: {e}")

    def index_urls(self, reindex=False):
        """
        Index URLs from the database table
        
        Args:
            reindex (bool): If True, reindex already indexed pages
        """
        offset = 0
        total_indexed = 0
        start_time = datetime.datetime.now()
        
        try:
            while not self.shutdown_requested:
                print(f"[BATCH]: Fetching batch from offset {offset}")
                batch = self.db.fetch_batch(self.table, batch_size=100, offset=offset, where_clause="status = 'processed'")
                
                if not batch:
                    print("[INFO] No more URLs to index. Process complete.")
                    break

                for row in batch:
                    if self.shutdown_requested:
                        break
                        
                    page_id = row['id']
                    url = row['url']
                    
                    # Ensure we have a status record for this page
                    self.create_or_update_index_status(page_id)
                    
                    # Check if URL has already been indexed
                    status = self.get_indexing_status(page_id)
                    
                    # Skip if already indexed and not reindexing
                    if status == "indexed" and not reindex:
                        print(f"[SKIPPED]: Already indexed URL: {url}")
                        continue
                    
                    # Update status to "indexing"
                    self.update_index_status(page_id, "indexing")
                    
                    try:
                        # Clear existing index if reindexing
                        if status == "indexed" and reindex:
                            self.clear_existing_index(page_id)
                        
                        # Extract text and keywords
                        print(f"[INDEXING]: {url}")
                        text = self.extract_text(url)
                        
                        if not text.strip():
                            self.update_index_status(page_id, "failed", "No text content found")
                            continue
                        
                        keywords = self.extract_keywords(text)
                        self.insert_keywords(keywords, page_id)
                        self.update_index_status(page_id, "indexed")
                        
                        total_indexed += 1
                        print(f"[INDEXED]: {len(keywords)} keywords for URL: {url}")
                        
                    except Exception as e:
                        error_msg = f"Failed to index: {str(e)}"
                        print(f"[ERROR]: {error_msg} for URL: {url}")
                        self.update_index_status(page_id, "failed", error_msg)

                offset += len(batch)
                
                # Save progress periodically
                if self.insert_buffer:
                    print(f"[SAVING]: {len(self.insert_buffer)} keyword entries")
                    self.db.insert_many("inverted_index", self.insert_buffer)
                    self.insert_buffer = []
        
        except KeyboardInterrupt:
            print("\n[INFO] Indexing interrupted by user.")
        
        finally:
            # Save any remaining data
            if self.insert_buffer:
                print(f"[SAVING]: Final batch of {len(self.insert_buffer)} keyword entries")
                self.db.insert_many("inverted_index", self.insert_buffer)
                self.insert_buffer = []
            
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            print(f"\n[SUMMARY] Indexed {total_indexed} URLs in {elapsed:.2f} seconds")
            print(f"[SUMMARY] Average: {total_indexed/elapsed:.2f} URLs/sec")
            
            if self.shutdown_requested:
                print("[INFO] Indexing paused. Run again to continue.")
            else:
                print("[INFO] Indexing completed successfully.")


def run_indexer(host, user, password, database):
    print("[INFO] Starting indexer...")
    db = DatabaseController(
        host=host,
        user=user,
        password=password,
        database=database
    )

    # Create index status tracking table
    db.create_table("indexing_status", {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",
        "page_id": "INT NOT NULL UNIQUE",
        "status": "ENUM('pending', 'indexing', 'indexed', 'failed') DEFAULT 'pending'",
        "last_indexed": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        "error": "VARCHAR(255) DEFAULT NULL",
        "FOREIGN KEY (page_id) REFERENCES crawler_queue(id) ON DELETE CASCADE": ""
    })

    # Create inverted index table if not exists
    db.create_table("inverted_index", {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",
        "keyword": "VARCHAR(255) NOT NULL",
        "page_id": "INT NOT NULL",
        "frequency": "INT NOT NULL DEFAULT 1",
        "FOREIGN KEY (page_id) REFERENCES crawler_queue(id) ON DELETE CASCADE": "",
        "INDEX (keyword)": ""
    })

    # Create the indexer
    indexer = ResumableIndexer(db, "crawler_queue")
    
    # Start indexing (set reindex=True to force reindex already indexed pages)
    indexer.index_urls(reindex=False)
    
    db.close()