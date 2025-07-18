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
        self.rapidapi_host = "newsnow.p.rapidapi.com"
        self.news_endpoint = "/newsv2_top_news_cat"
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', '30'))
        self.read_timeout = int(os.getenv('READ_TIMEOUT', '30'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.max_pages = int(os.getenv('MAX_PAGES', '3'))
        
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
                name='Fetch News from NewsNow API',
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
        """Fetch news from NewsNow API with your exact working format"""
        headers = {
            'x-rapidapi-key': self.rapidapi_key,
            'x-rapidapi-host': self.rapidapi_host,
            'Content-Type': 'application/json'
        }
        
        # Use the exact payload format that works
        payload = json.dumps({
            "category": "BUSINESS",  # Fixed typo from BUSSINESS
            "location": "in",
            "language": "en",
            "page": page
        })
        
        for attempt in range(self.max_retries):
            start_time = time.time()
            conn = None
            
            try:
                logger.info(f"Fetching news from NewsNow API page {page} (attempt {attempt + 1}/{self.max_retries})")
                
                conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=self.connection_timeout)
                conn.request("POST", self.news_endpoint, payload, headers)
                
                response = conn.getresponse()
                response_time = time.time() - start_time
                
                if response.status == 200:
                    data = response.read()
                    response_text = data.decode("utf-8")
                    
                    logger.info(f"Successfully fetched NewsNow data for page {page} in {response_time:.2f}s")
                    
                    # Parse the response as key-value format (not JSON)
                    response_data = self.parse_kv_response(response_text)
                    
                    # Log successful API call
                    articles_count = response_data.get('count', 0)
                    db.log_api_call(self.news_endpoint, response.status, response_time, articles_count, page)
                    
                    return response_data
                
                elif response.status == 429:
                    logger.warning(f"Rate limit exceeded (429). Attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    continue
                
                else:
                    error_msg = f"NewsNow API request failed with status {response.status}"
                    logger.error(error_msg)
                    
                    # Log failed API call
                    db.log_api_call(self.news_endpoint, response.status, response_time, 0, page)
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        raise NewsAPIError(error_msg)
            
            except Exception as e:
                logger.error(f"Connection error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise NewsAPIError(f"Connection failed after {self.max_retries} attempts: {e}")
            
            finally:
                if conn:
                    conn.close()
        
        return None
    
    def parse_kv_response(self, response_text: str) -> Dict[str, Any]:
        """Parse the key-value response format from NewsNow API"""
        result = {}
        lines = response_text.strip().split('\n')
        
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip().strip('"')
                
                # Handle special keys
                if key == 'count':
                    result[key] = int(value) if value.isdigit() else 0
                elif key.startswith('news:') and key.endswith(':'):
                    # This marks the start of a news item
                    continue
                else:
                    result[key] = value
        
        return result
    
    def parse_news_data(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse and normalize news data from NewsNow API response"""
        articles = []
        
        try:
            lines = response_text.strip().split('\n')
            current_article = {}
            article_index = None
            
            # Parse the key-value response format
            for line in lines:
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip().strip('"')
                    
                    if key == 'count':
                        total_count = int(value) if value.isdigit() else 0
                        logger.info(f"Processing {total_count} news items from NewsNow")
                        continue
                    
                    # Check if this is a news item key
                    if key.startswith('news:') and ':' in key:
                        parts = key.split(':')
                        if len(parts) >= 3:
                            index = parts[1]
                            field = parts[2]
                            
                            # If we're starting a new article, save the previous one
                            if index != article_index and current_article:
                                article = self.normalize_article(current_article)
                                if article:
                                    articles.append(article)
                                current_article = {}
                            
                            article_index = index
                            
                            if field == 'title':
                                current_article['title'] = value
                            elif field == 'url':
                                current_article['url'] = value
                            elif field == 'top_image':
                                current_article['thumbnail'] = value
                            elif field == 'date':
                                current_article['published_date'] = value
                            elif field == 'short_description':
                                current_article['summary'] = value
                            elif field == 'text':
                                current_article['full_content'] = value
                            elif field == 'publisher':
                                # Handle publisher object
                                if 'publisher' not in current_article:
                                    current_article['publisher'] = {}
                            elif key.startswith('news:') and 'publisher:' in key:
                                pub_field = key.split('publisher:')[1]
                                if 'publisher' not in current_article:
                                    current_article['publisher'] = {}
                                current_article['publisher'][pub_field] = value
            
            # Don't forget the last article
            if current_article:
                article = self.normalize_article(current_article)
                if article:
                    articles.append(article)
            
            logger.info(f"Successfully parsed {len(articles)} articles from NewsNow")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing NewsNow data: {e}")
            return []
    
    def normalize_article(self, raw_article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize article data to standard format"""
        try:
            title = raw_article.get('title', '').strip()
            url = raw_article.get('url', '').strip()
            
            # Skip articles with missing essential fields
            if not title or not url:
                return None
            
            # Extract publisher info
            publisher_info = raw_article.get('publisher', {})
            if isinstance(publisher_info, dict):
                publisher = publisher_info.get('title', '') or publisher_info.get('href', '')
            else:
                publisher = 'NewsNow'
            
            # Convert date format
            published_date = raw_article.get('published_date', '')
            formatted_date = published_date
            if published_date:
                try:
                    # Parse: "Fri, 18 Jul 2025 06:25:48 GMT"
                    dt = datetime.strptime(published_date, '%a, %d %b %Y %H:%M:%S %Z')
                    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    logger.debug(f"Could not parse date {published_date}: {e}")
                    formatted_date = published_date
            
            return {
                'title': title,
                'url': url,
                'publisher': publisher,
                'published_date': formatted_date,
                'summary': raw_article.get('summary', ''),
                'thumbnail': raw_article.get('thumbnail', ''),
                'language': 'en',
                'category': 'business',
                'full_content': raw_article.get('full_content', '')
            }
            
        except Exception as e:
            logger.warning(f"Error normalizing article: {e}")
            return None
    
    def fetch_and_store_news(self):
        """Main function to fetch news from multiple pages and store in database"""
        try:
            logger.info("Starting NewsNow multi-page news fetch and store process")
            
            total_articles = 0
            fetch_timestamp = int(time.time())
            
            # Fetch pages in reverse order (3, 2, 1) so newer articles appear first
            for page in range(self.max_pages, 0, -1):
                try:
                    logger.info(f"Fetching page {page}/{self.max_pages}")
                    
                    # Fetch news from NewsNow API
                    raw_response = self.fetch_news_from_api(page)
                    if not raw_response:
                        logger.warning(f"No data received from NewsNow API for page {page}")
                        continue
                    
                    # Get the raw response text for parsing
                    response_text = self.get_raw_response_text(page)
                    if not response_text:
                        logger.warning(f"Could not get raw response for page {page}")
                        continue
                    
                    # Parse the data
                    articles = self.parse_news_data(response_text)
                    if not articles:
                        logger.warning(f"No articles parsed from NewsNow response for page {page}")
                        continue
                    
                    # Store articles in database with fetch order
                    page_fetch_order = fetch_timestamp + (page * 1000)
                    inserted_count = db.bulk_insert_articles(articles, page_fetch_order)
                    
                    total_articles += inserted_count
                    logger.info(f"Page {page}: Stored {inserted_count} articles")
                    
                    # Delay between pages
                    if page > 1:
                        time.sleep(3)
                        
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
    
    def get_raw_response_text(self, page: int) -> Optional[str]:
        """Get raw response text for parsing"""
        headers = {
            'x-rapidapi-key': self.rapidapi_key,
            'x-rapidapi-host': self.rapidapi_host,
            'Content-Type': 'application/json'
        }
        
        payload = json.dumps({
            "category": "BUSINESS",
            "location": "in",
            "language": "en",
            "page": page
        })
        
        conn = None
        try:
            conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=self.connection_timeout)
            conn.request("POST", self.news_endpoint, payload, headers)
            
            response = conn.getresponse()
            if response.status == 200:
                data = response.read()
                return data.decode("utf-8")
        except Exception as e:
            logger.error(f"Error getting raw response: {e}")
        finally:
            if conn:
                conn.close()
        
        return None
    
    def cleanup_database(self):
        """Clean up old articles and excess articles from database"""
        try:
            logger.info("Starting database cleanup process")
            
            old_deleted = db.cleanup_old_articles()
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
            logger.info("Manual NewsNow fetch triggered")
            self.fetch_and_store_news()
            return {'status': 'success', 'message': 'NewsNow fetch completed successfully'}
        except Exception as e:
            logger.error(f"Manual fetch failed: {e}")
            return {'status': 'error', 'message': str(e)}

# Global news fetcher instance
news_fetcher = NewsFetcher()
