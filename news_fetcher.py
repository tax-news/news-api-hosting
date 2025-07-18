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
        self.rapidapi_host = os.getenv('RAPIDAPI_HOST', 'reuters-api.p.rapidapi.com')
        self.connection_timeout = int(os.getenv('CONNECTION_TIMEOUT', '30'))
        self.read_timeout = int(os.getenv('READ_TIMEOUT', '30'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        
        # Reuters categories to fetch from
        self.reuters_categories = [
            'https%3A%2F%2Fwww.reuters.com%2Fbusiness%2F',
            'https%3A%2F%2Fwww.reuters.com%2Ftechnology%2F',
            'https%3A%2F%2Fwww.reuters.com%2Fmarkets%2F'
        ]
        
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
                name='Fetch News from Reuters API',
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
    
    def fetch_news_from_reuters(self, category_url: str) -> Optional[Dict[str, Any]]:
        """Fetch news from Reuters API for a specific category"""
        headers = {
            'x-rapidapi-key': self.rapidapi_key,
            'x-rapidapi-host': self.rapidapi_host
        }
        
        endpoint_url = f"/category?url={category_url}"
        
        for attempt in range(self.max_retries):
            start_time = time.time()
            conn = None
            
            try:
                logger.info(f"Fetching news from Reuters API: {category_url} (attempt {attempt + 1}/{self.max_retries})")
                
                conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=self.connection_timeout)
                conn.request("GET", endpoint_url, headers=headers)
                
                response = conn.getresponse()
                response_time = time.time() - start_time
                
                if response.status == 200:
                    data = response.read()
                    response_data = json.loads(data.decode("utf-8"))
                    
                    logger.info(f"Successfully fetched Reuters data in {response_time:.2f}s")
                    
                    # Log successful API call
                    articles_count = len(response_data) if isinstance(response_data, list) else 0
                    db.log_api_call(endpoint_url, response.status, response_time, articles_count, 1)
                    
                    return response_data
                
                elif response.status == 429:
                    logger.warning(f"Rate limit exceeded (429). Attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    continue
                
                else:
                    error_msg = f"Reuters API request failed with status {response.status}"
                    logger.error(error_msg)
                    
                    # Log failed API call
                    db.log_api_call(endpoint_url, response.status, response_time, 0, 1)
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        raise NewsAPIError(error_msg)
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise NewsAPIError(f"Invalid JSON response: {e}")
            
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
    
    def parse_reuters_data(self, raw_data: Any, category: str) -> List[Dict[str, Any]]:
        """Parse and normalize news data from Reuters API response"""
        articles = []
        
        try:
            if not isinstance(raw_data, list):
                logger.warning("Unexpected Reuters response format - expected list")
                return articles
            
            logger.info(f"Processing {len(raw_data)} Reuters articles from {category}")
            
            for item in raw_data:
                try:
                    if not isinstance(item, dict):
                        continue
                    
                    # Extract article data from Reuters format
                    title = item.get('title', '').strip()
                    url = item.get('url', '').strip()
                    summary = item.get('summary', '').strip() or item.get('description', '').strip()
                    published_date = item.get('published_date', '').strip() or item.get('date', '').strip()
                    thumbnail = item.get('image', '').strip() or item.get('thumbnail', '').strip()
                    author = item.get('author', '').strip()
                    
                    # Skip articles with missing essential fields
                    if not title or not url:
                        logger.warning(f"Skipping article with missing title or URL")
                        continue
                    
                    # Convert date format if needed
                    formatted_date = published_date
                    if published_date:
                        try:
                            # Try to parse various date formats
                            for date_format in ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                                try:
                                    dt = datetime.strptime(published_date, date_format)
                                    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                                    break
                                except ValueError:
                                    continue
                        except Exception as e:
                            logger.debug(f"Could not parse date {published_date}: {e}")
                            formatted_date = published_date
                    
                    # Create normalized article
                    article = {
                        'title': title,
                        'url': url,
                        'publisher': 'Reuters',
                        'published_date': formatted_date,
                        'summary': summary,
                        'thumbnail': thumbnail,
                        'language': 'en',
                        'category': 'business',
                        'full_content': item.get('content', '').strip() or summary
                    }
                    
                    articles.append(article)
                    logger.debug(f"Added Reuters article: {title[:50]}...")
                    
                except Exception as e:
                    logger.warning(f"Error parsing Reuters article: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(articles)} Reuters articles")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing Reuters data: {e}")
            return []
    
    def fetch_and_store_news(self):
        """Main function to fetch news from Reuters and store in database"""
        try:
            logger.info("Starting Reuters news fetch and store process")
            
            total_articles = 0
            fetch_timestamp = int(time.time())
            
            # Fetch from different Reuters categories
            for i, category_url in enumerate(self.reuters_categories):
                try:
                    category_name = category_url.split('%2F')[-2] if '%2F' in category_url else f"category_{i+1}"
                    logger.info(f"Fetching from Reuters category: {category_name}")
                    
                    # Fetch news from Reuters API
                    raw_data = self.fetch_news_from_reuters(category_url)
                    if not raw_data:
                        logger.warning(f"No data received from Reuters API for {category_name}")
                        continue
                    
                    # Parse the data
                    articles = self.parse_reuters_data(raw_data, category_name)
                    if not articles:
                        logger.warning(f"No articles parsed from Reuters response for {category_name}")
                        continue
                    
                    # Store articles in database with fetch order
                    # Higher category index gets higher fetch_order so newer categories appear first
                    category_fetch_order = fetch_timestamp + ((len(self.reuters_categories) - i) * 1000)
                    inserted_count = db.bulk_insert_articles(articles, category_fetch_order)
                    
                    total_articles += inserted_count
                    logger.info(f"Category {category_name}: Stored {inserted_count} articles")
                    
                    # Delay between categories to avoid rate limits
                    if i < len(self.reuters_categories) - 1:
                        time.sleep(3)
                        
                except Exception as e:
                    logger.error(f"Error processing Reuters category {i+1}: {e}")
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
            logger.info("Manual Reuters news fetch triggered")
            self.fetch_and_store_news()
            return {'status': 'success', 'message': 'Reuters news fetch completed successfully'}
        except Exception as e:
            logger.error(f"Manual fetch failed: {e}")
            return {'status': 'error', 'message': str(e)}

# Global news fetcher instance
news_fetcher = NewsFetcher()
