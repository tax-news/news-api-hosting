import http.client
import json
import os
import time
import logging
import datetime
from datetime import datetime
from typing import Dict, List, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from database import db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.getenv('LOG_FILE', 'news_api.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NewsAPIError(Exception):
    """Custom exception for news API errors"""
    pass

class NewsFetcher:
    def __init__(self):
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY')
        self.rapidapi_host = os.getenv('RAPIDAPI_HOST', 'newsnow.p.rapidapi.com')
        self.news_endpoint = os.getenv('NEWS_ENDPOINT', '/news')
        self.news_language = os.getenv('NEWS_LANGUAGE', 'en')
        self.news_category = os.getenv('NEWS_CATEGORY', 'business')
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', '30'))
        self.read_timeout = int(os.getenv('READ_TIMEOUT', '30'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.max_pages = int(os.getenv('MAX_PAGES', '3'))  # Reduced to 3 pages
        
        if not self.rapidapi_key:
            raise ValueError("RAPIDAPI_KEY is required in environment variables")
        
        self.scheduler = BackgroundScheduler()
        self.setup_scheduler()
    
    def setup_scheduler(self):
        """Setup background scheduler for fetching news and cleanup"""
        try:
            # Schedule news fetching every 2 hours
            fetch_interval = int(os.getenv('FETCH_INTERVAL_HOURS', '2'))
            self.scheduler.add_job(
                func=self.fetch_and_store_news,
                trigger=IntervalTrigger(hours=fetch_interval),
                id='fetch_news',
                name='Fetch News from API',
                replace_existing=True
            )
            
            # Schedule cleanup every 4 hours
            cleanup_interval = int(os.getenv('CLEANUP_INTERVAL_HOURS', '4'))
            self.scheduler.add_job(
                func=self.cleanup_database,
                trigger=IntervalTrigger(hours=cleanup_interval),
                id='cleanup_database',
                name='Cleanup Database',
                replace_existing=True
            )
            
            logger.info(f"Scheduler configured: Fetch every {fetch_interval}h, Cleanup every {cleanup_interval}h")
            
        except Exception as e:
            logger.error(f"Error setting up scheduler: {e}")
            raise
    
    def start_scheduler(self):
        """Start the background scheduler"""
        try:
            if not self.scheduler.running:
                self.scheduler.start()
                logger.info("News fetcher scheduler started successfully")
                
                # Run initial fetch
                self.fetch_and_store_news()
            else:
                logger.info("Scheduler is already running")
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            raise
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("News fetcher scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def fetch_news_from_api(self, page: int = 1) -> Optional[Dict[str, Any]]:
        """Fetch news from external API with retry logic"""
        headers = {
            'x-rapidapi-key': self.rapidapi_key,
            'x-rapidapi-host': self.rapidapi_host
        }
        
        # Try different endpoint formats
        endpoints_to_try = [
            f"/news?category={self.news_category}&language={self.news_language}&page={page}",
            f"/news?category={self.news_category}&page={page}",
            f"/newsv2_top_news_cat"
        ]
        
        for endpoint_url in endpoints_to_try:
            logger.info(f"Trying endpoint: {endpoint_url}")
            
            for attempt in range(self.max_retries):
                start_time = time.time()
                conn = None
                
                try:
                    logger.info(f"Fetching news from {endpoint_url} (attempt {attempt + 1}/{self.max_retries})")
                    
                    conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=self.connection_timeout)
                    
                    # Try GET first
                    if "newsv2_top_news_cat" in endpoint_url:
                        # Use POST for this specific endpoint
                        payload = json.dumps({
                            "category": self.news_category.upper(),
                            "location": "",
                            "language": self.news_language,
                            "page": page
                        })
                        headers['Content-Type'] = 'application/json'
                        conn.request("POST", endpoint_url, payload, headers)
                    else:
                        # Use GET for other endpoints
                        if 'Content-Type' in headers:
                            del headers['Content-Type']
                        conn.request("GET", endpoint_url, headers=headers)
                    
                    response = conn.getresponse()
                    response_time = time.time() - start_time
                    
                    if response.status == 200:
                        data = response.read()
                        response_data = json.loads(data.decode("utf-8"))
                        
                        logger.info(f"Successfully fetched news data from {endpoint_url} in {response_time:.2f}s")
                        
                        # Log successful API call
                        articles_count = self.count_articles_in_response(response_data)
                        db.log_api_call(endpoint_url, response.status, response_time, articles_count, page)
                        
                        return response_data
                    
                    elif response.status == 429:
                        logger.warning(f"Rate limit exceeded (429). Attempt {attempt + 1}")
                        if attempt < self.max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                        continue
                    
                    elif response.status == 404:
                        logger.warning(f"Endpoint {endpoint_url} not found (404), trying next endpoint")
                        db.log_api_call(endpoint_url, response.status, response_time, 0, page)
                        break  # Try next endpoint
                    
                    else:
                        error_msg = f"API request failed with status {response.status}"
                        logger.error(error_msg)
                        db.log_api_call(endpoint_url, response.status, response_time, 0, page)
                        
                        if attempt < self.max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                        else:
                            break  # Try next endpoint
                
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        break  # Try next endpoint
                
                except Exception as e:
                    logger.error(f"Connection error: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        break  # Try next endpoint
                
                finally:
                    if conn:
                        conn.close()
        
        logger.error("All endpoints failed")
        return None
    
    def count_articles_in_response(self, raw_data: Dict[str, Any]) -> int:
        """Count articles in response data"""
        if isinstance(raw_data, dict):
            # Try different response formats
            if 'count' in raw_data:
                return raw_data.get('count', 0)
            elif 'articles' in raw_data:
                return len(raw_data.get('articles', []))
            elif 'data' in raw_data:
                return len(raw_data.get('data', []))
            else:
                # Count news items in NewsNow format
                count = 0
                i = 0
                while f"news:{i}:" in str(raw_data):
                    count += 1
                    i += 1
                return count
        return 0
    
    def parse_news_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and normalize news data from API response"""
        articles = []
        
        try:
            if not isinstance(raw_data, dict):
                logger.warning("Unexpected response format")
                return articles
            
            # Handle different response formats
            if 'articles' in raw_data:
                # Standard news API format
                news_items = raw_data.get('articles', [])
                logger.info(f"Processing {len(news_items)} articles from standard format")
                
                for item in news_items:
                    article = {
                        'title': item.get('title', '').strip(),
                        'url': item.get('url', '').strip(),
                        'publisher': item.get('source', {}).get('name', '').strip() if isinstance(item.get('source'), dict) else item.get('publisher', '').strip(),
                        'published_date': item.get('publishedAt', '').strip(),
                        'summary': item.get('description', '').strip(),
                        'thumbnail': item.get('urlToImage', '').strip(),
                        'language': self.news_language,
                        'category': 'business',
                        'full_content': item.get('content', '').strip()
                    }
                    
                    if article['title'] and article['url']:
                        articles.append(article)
            
            elif 'count' in raw_data:
                # NewsNow API format
                count = raw_data.get('count', 0)
                if count == 0:
                    logger.warning("No articles found in API response")
                    return articles
                
                logger.info(f"Processing {count} news items from NewsNow format")
                
                for i in range(count):
                    news_key = f"news:{i}:"
                    
                    title = raw_data.get(f"{news_key}title", "").strip().strip('"')
                    url = raw_data.get(f"{news_key}url", "").strip().strip('"')
                    top_image = raw_data.get(f"{news_key}top_image", "").strip().strip('"')
                    date = raw_data.get(f"{news_key}date", "").strip().strip('"')
                    short_description = raw_data.get(f"{news_key}short_description", "").strip().strip('"')
                    text = raw_data.get(f"{news_key}text", "").strip().strip('"')
                    
                    publisher_href = raw_data.get(f"{news_key}publisher:href", "").strip().strip('"')
                    publisher_title = raw_data.get(f"{news_key}publisher:title", "").strip().strip('"')
                    
                    if not title or not url:
                        logger.warning(f"Skipping article with missing title or URL")
                        continue
                    
                    # Convert date format if possible
                    published_date = date
                    if date:
                        try:
                            dt = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z')
                            published_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Could not parse date {date}: {e}")
                            published_date = date
                    
                    article = {
                        'title': title,
                        'url': url,
                        'publisher': publisher_title or publisher_href,
                        'published_date': published_date,
                        'summary': short_description or text[:200] + "..." if text else "",
                        'thumbnail': top_image,
                        'language': self.news_language,
                        'category': 'business',
                        'full_content': text
                    }
                    
                    articles.append(article)
                    logger.debug(f"Added article: {title[:50]}...")
            
            else:
                # Try to handle other formats
                logger.warning("Unknown response format, trying to extract data")
                logger.debug(f"Response keys: {list(raw_data.keys())}")
            
            logger.info(f"Successfully parsed {len(articles)} articles")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing news data: {e}")
            return []
    
    def fetch_and_store_news(self):
        """Main function to fetch news from multiple pages and store in database"""
        try:
            logger.info("Starting multi-page news fetch and store process")
            
            total_articles = 0
            fetch_timestamp = int(time.time())
            
            # Fetch pages in reverse order (3, 2, 1) for better ordering
            for page in range(self.max_pages, 0, -1):
                try:
                    logger.info(f"Fetching page {page}/{self.max_pages}")
                    
                    # Fetch news from API
                    raw_data = self.fetch_news_from_api(page)
                    if not raw_data:
                        logger.warning(f"No data received from API for page {page}")
                        continue
                    
                    # Parse the data
                    articles = self.parse_news_data(raw_data)
                    if not articles:
                        logger.warning(f"No articles parsed from API response for page {page}")
                        continue
                    
                    # Store articles in database with fetch order
                    page_fetch_order = fetch_timestamp + (page * 1000)
                    inserted_count = db.bulk_insert_articles(articles, page_fetch_order)
                    
                    total_articles += inserted_count
                    logger.info(f"Page {page}: Stored {inserted_count} articles")
                    
                    # Small delay between pages
                    if page > 1:
                        time.sleep(2)
                        
                except Exception as e:
                    logger.error(f"Error processing page {page}: {e}")
                    continue
            
            logger.info(f"Total articles stored in this fetch: {total_articles}")
            
            # Cleanup excess articles if needed
            self.cleanup_database()
            
            # Log summary
            stats = db.get_database_stats()
            logger.info(f"Database stats: {stats}")
            
        except NewsAPIError as e:
            logger.error(f"News API error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in fetch_and_store_news: {e}")
    
    def cleanup_database(self):
        """Clean up old articles and excess articles from database"""
        try:
            logger.info("Starting database cleanup process")
            
            # Clean up articles older than retention period
            old_deleted = db.cleanup_old_articles()
            
            # Clean up excess articles if count exceeds limit
            excess_deleted = db.cleanup_excess_articles()
            
            total_deleted = old_deleted + excess_deleted
            if total_deleted > 0:
                logger.info(f"Cleanup completed: {total_deleted} articles removed ({old_deleted} old, {excess_deleted} excess)")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status and job information"""
        try:
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
            
            return {
                'scheduler_running': self.scheduler.running,
                'jobs': jobs,
                'timezone': str(self.scheduler.timezone)
            }
        except Exception as e:
            logger.error(f"Error getting scheduler status: {e}")
            return {'error': str(e)}
    
    def manual_fetch(self) -> Dict[str, Any]:
        """Manually trigger news fetch (for testing/admin purposes)"""
        try:
            logger.info("Manual news fetch triggered")
            self.fetch_and_store_news()
            return {'status': 'success', 'message': 'News fetch completed successfully'}
        except Exception as e:
            logger.error(f"Manual fetch failed: {e}")
            return {'status': 'error', 'message': str(e)}

# Global news fetcher instance
news_fetcher = NewsFetcher()
