import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from collections import deque
import concurrent.futures
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from database.db import DatabaseController
import datetime

class ResumableCrawler:
    def __init__(self, seed_urls, max_workers=10, timeout=5, blacklist=None, db=None, buffer_limit=50):
        self.seed_urls = seed_urls
        self.blacklist = blacklist or []
        self.visited = set()
        self.queue = deque()
        self.max_workers = max_workers
        self.timeout = timeout

        self.db = db  # Database controller instance
        self.insert_buffer = []
        self.insert_buffer_limit = buffer_limit

        # Setup session with retry strategy
        self.session = self._create_session()
        
        # Load state from database or initialize with seed URLs
        self.resume_from_db()
    
    def is_blacklisted(self, url):
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        for blocked in self.blacklist:
            if blocked.lower() in domain:
                return True
        return False

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

    def normalize_url(self, url):
        parsed = urlparse(url)
        normalized = parsed._replace(query="", fragment="")
        return urlunparse(normalized)
    
    def save_url_to_queue(self, url, status="pending"):
        """Save a URL to the queue database table"""
        self.insert_buffer.append({
            "url": url,
            "status": status,
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        if len(self.insert_buffer) >= self.insert_buffer_limit:
            self.db.insert_many("crawler_queue", self.insert_buffer)
            self.insert_buffer.clear()
    
    def mark_url_as_processed(self, url):
        """Mark a URL as processed in the database"""
        try:
            cursor = self.db.connection.cursor()
            sql = "UPDATE crawler_queue SET status = 'processed', timestamp = %s WHERE url = %s"
            cursor.execute(sql, (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), url))
            self.db.connection.commit()
            cursor.close()
        except Exception as e:
            print(f"Error marking URL as processed: {e}")
    
    def resume_from_db(self):
        """Load crawler state from database"""
        try:
            # Check if we have any data in the queue table
            cursor = self.db.connection.cursor(dictionary=True)
            
            # First, load all processed URLs into visited set
            cursor.execute("SELECT url FROM crawler_queue WHERE status = 'processed'")
            processed_urls = cursor.fetchall()
            
            if processed_urls:
                print(f"Found {len(processed_urls)} previously processed URLs")
                for row in processed_urls:
                    self.visited.add(row['url'])
            
            # Then load pending URLs into queue
            cursor.execute("SELECT url FROM crawler_queue WHERE status = 'pending'")
            pending_urls = cursor.fetchall()
            
            if pending_urls:
                print(f"Resuming crawl with {len(pending_urls)} pending URLs")
                for row in pending_urls:
                    self.queue.append(row['url'])
            else:
                print("No pending URLs found. Starting with seed URLs.")
                for url in self.seed_urls:
                    normalized_url = self.normalize_url(url)
                    self.queue.append(normalized_url)
                    self.save_url_to_queue(normalized_url)
            
            cursor.close()
            
        except Exception as e:
            print(f"Error loading state from database: {e}")
            print("Starting fresh with seed URLs")
            # If there's an error, start with seed URLs
            for url in self.seed_urls:
                normalized_url = self.normalize_url(url)
                self.queue.append(normalized_url)
                self.save_url_to_queue(normalized_url)

    def extract_external_links(self, url):
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = []

            base_domain = urlparse(url).netloc

            # Find all anchor tags with href attributes
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                parsed_url = urlparse(full_url)

                # Only consider HTTP/HTTPS links
                if not parsed_url.scheme.startswith('http'):
                    continue
                    
                # Skip internal links
                if parsed_url.netloc == base_domain:
                    continue
                
                normalized_url = self.normalize_url(full_url)
                links.append(normalized_url)
                
            return url, links
            
        except requests.exceptions.RequestException as e:
            print(f"[Exception]: Error fetching {url}: {e}")
        except Exception as e:
            print(f"[Exception]: An error occurred with {url}: {e}")
        
        return url, []

    def crawl(self):
        start_time = time.time()
        urls_crawled = 0
        
        try:     
            # Use ThreadPoolExecutor for concurrent requests
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                while self.queue:
                    # Get batch of URLs to process
                    batch_size = min(self.max_workers, len(self.queue))
                    batch = []
                        
                    for _ in range(batch_size):
                        if not self.queue:
                            break
                            
                        url = self.queue.popleft()
                            
                        if self.is_blacklisted(url):
                            # Mark blacklisted URLs as processed so we don't retry them
                            self.mark_url_as_processed(url)
                            continue

                        normalized_url = self.normalize_url(url)
                        if normalized_url not in self.visited:
                            self.visited.add(normalized_url)
                            batch.append(normalized_url)
                            urls_crawled += 1
                                     
                    if not batch:
                        continue
                            
                    # Submit batch for concurrent processing
                    future_to_url = {executor.submit(self.extract_external_links, url): url for url in batch}
                        
                    last_reported = 0
                    for future in concurrent.futures.as_completed(future_to_url):
                        url, links = future.result()
                        
                        # Mark URL as processed
                        self.mark_url_as_processed(url)
                            
                        print(f"[Crawled]: {url} -> Found {len(links)} external links")            
                        # Add new links to queue
                        for link in links:
                            if (not self.is_blacklisted(link) 
                                and link not in self.visited 
                                and link not in self.queue):
                                self.queue.append(link)
                                self.save_url_to_queue(link)
                            
                        # Print progress
                        if len(self.visited) >= last_reported + 10:
                            elapsed = time.time() - start_time
                            print(f"Crawled {len(self.visited)} URLs in {elapsed:.2f} seconds ({len(self.visited)/elapsed:.2f} URLs/sec)")
                            last_reported = len(self.visited)
                
        except KeyboardInterrupt:
            print("Crawl interrupted by user. Progress is saved to database. Run again to resume.")
        finally:
            # Save any remaining items in buffer
            if self.insert_buffer:
                self.db.insert_many("crawler_queue", self.insert_buffer)
                self.insert_buffer.clear()

            elapsed = time.time() - start_time
            print(f"\nCrawl completed or paused: {urls_crawled} URLs in {elapsed:.2f} seconds ({urls_crawled/elapsed:.2f} URLs/sec)")
            print(f"Queue size at exit: {len(self.queue)}")
            print(f"Total unique URLs visited: {len(self.visited)}")
            
            # Close the session
            self.session.close()

def run_crawler(host, user, password, database):
    print("Starting Crawler...")
    db = DatabaseController(
        host=host,
        user=user,
        password=password,
        database=database
    )

    # Create table for crawler queue with status tracking
    db.create_table("crawler_queue", {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",
        "url": "VARCHAR(255) NOT NULL UNIQUE",
        "status": "ENUM('pending', 'processed') DEFAULT 'pending'",
        "timestamp": "DATETIME DEFAULT CURRENT_TIMESTAMP"
    })

    seed_urls = [
        "https://en.wikipedia.org/wiki/Hololive_Production", 
        "https://en.wikipedia.org/wiki/Super_(gamer)"
    ]
    
    blacklist = [
        "web.archive.org",
        "archive.org",
        "example.com"
    ] 

    crawler = ResumableCrawler(
        seed_urls=seed_urls,
        max_workers=20,  
        timeout=5,
        blacklist=blacklist,
        db=db
    )

    crawler.crawl()
    db.close()