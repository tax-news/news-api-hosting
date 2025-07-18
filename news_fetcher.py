import http.client
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from database import db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsFetcher:
    def __init__(self):
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY', '9c0fffe9f2msh60e44a23038dea1p115944jsn78702b89602e')
        self.scheduler = BackgroundScheduler()
        self.setup_scheduler()
    
    def setup_scheduler(self):
        """Setup background scheduler"""
        try:
            fetch_interval = int(os.getenv('FETCH_INTERVAL_HOURS', '2'))
            self.scheduler.add_job(
                func=self.fetch_and_store_news,
                trigger=IntervalTrigger(hours=fetch_interval),
                id='fetch_news',
                name='Fetch News from NewsNow API',
                replace_existing=True
            )
            
            cleanup_interval = int(os.getenv('CLEANUP_INTERVAL_HOURS', '4'))
            self.scheduler.add_job(
                func=self.cleanup_database,
                trigger=IntervalTrigger(hours=cleanup_interval),
                id='cleanup_database',
                name='Cleanup Database',
                replace_existing=True
            )
            
            logger.info(f"Scheduler: Fetch every {fetch_interval}h, Cleanup every {cleanup_interval}h")
            
        except Exception as e:
            logger.error(f"Error setting up scheduler: {e}")
    
    def start_scheduler(self):
        """Start the scheduler"""
        try:
            if not self.scheduler.running:
                self.scheduler.start()
                logger.info("Scheduler started")
                # Run initial fetch
                self.fetch_and_store_news()
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def fetch_news_page(self, page: int) -> Optional[str]:
        """Fetch news from NewsNow API with very conservative rate limiting"""
        max_retries = 3
        base_wait = 10  # Start with 10 seconds
        
        for attempt in range(max_retries):
            try:
                # Wait before each request (even first attempt)
                if attempt > 0:
                    wait_time = base_wait * (2 ** attempt)  # 10s, 20s, 40s
                    logger.info(f"Waiting {wait_time}s before retry {attempt + 1}...")
                    time.sleep(wait_time)
                else:
                    # Always wait 3 seconds before first attempt
                    time.sleep(3)
                
                conn = http.client.HTTPSConnection("newsnow.p.rapidapi.com")
                
                payload = json.dumps({
                    "category": "BUSINESS",
                    "location": "in", 
                    "language": "en",
                    "page": page
                })
                
                headers = {
                    'x-rapidapi-key': self.rapidapi_key,
                    'x-rapidapi-host': "newsnow.p.rapidapi.com",
                    'Content-Type': "application/json"
                }
                
                logger.info(f"Attempting to fetch page {page} (attempt {attempt + 1})")
                
                conn.request("POST", "/newsv2_top_news_cat", payload, headers)
                res = conn.getresponse()
                
                if res.status == 200:
                    data = res.read()
                    response_text = data.decode("utf-8")
                    logger.info(f"✅ Successfully fetched page {page}")
                    return response_text
                elif res.status == 429:
                    logger.warning(f"❌ Rate limit (429) for page {page}, attempt {attempt + 1}")
                    # Don't return None immediately, let the retry logic handle it
                    if attempt == max_retries - 1:
                        logger.error(f"🚫 Completely rate limited after {max_retries} attempts")
                        return None
                else:
                    logger.error(f"❌ API error {res.status} for page {page}")
                    if attempt == max_retries - 1:
                        return None
                    
            except Exception as e:
                logger.error(f"❌ Connection error for page {page}: {e}")
                if attempt == max_retries - 1:
                    return None
            finally:
                try:
                    conn.close()
                except:
                    pass
        
        return None
    
    def parse_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse your exact response format"""
        articles = []
        
        try:
            lines = response_text.strip().split('\n')
            current_article = {}
            article_count = 0
            
            for line in lines:
                line = line.strip()
                if not line or ':' not in line:
                    continue
                    
                if line.startswith('count:'):
                    article_count = int(line.split(':')[1])
                    logger.info(f"Found {article_count} articles in response")
                    continue
                
                # Parse news items: news:0:title:"Something"
                if line.startswith('news:'):
                    parts = line.split(':', 3)  # Split into max 4 parts
                    if len(parts) >= 3:
                        news_index = parts[1]
                        field_name = parts[2]
                        field_value = parts[3] if len(parts) > 3 else ""
                        
                        # Clean up the value (remove quotes)
                        if field_value.startswith('"') and field_value.endswith('"'):
                            field_value = field_value[1:-1]
                        
                        # Store the field in current article
                        if field_name == 'title':
                            current_article['title'] = field_value
                        elif field_name == 'url':
                            current_article['url'] = field_value
                        elif field_name == 'top_image':
                            current_article['thumbnail'] = field_value
                        elif field_name == 'date':
                            current_article['date'] = field_value
                        elif field_name == 'short_description':
                            current_article['summary'] = field_value
                        elif field_name == 'text':
                            current_article['content'] = field_value
                        elif field_name == 'publisher':
                            # Handle publisher info
                            pass
                
                # Handle publisher fields: news:0:publisher:title:"NDTV Profit"
                elif 'publisher:' in line:
                    parts = line.split(':', 4)
                    if len(parts) >= 4:
                        field_value = parts[4] if len(parts) > 4 else ""
                        if field_value.startswith('"') and field_value.endswith('"'):
                            field_value = field_value[1:-1]
                        
                        if 'title' in line:
                            current_article['publisher'] = field_value
            
            # Create articles from the parsed data
            # Since we have all fields mixed together, we need to reconstruct articles
            # Let's just take the first article's data for now
            if current_article and current_article.get('title') and current_article.get('url'):
                # Convert date format
                published_date = current_article.get('date', '')
                if published_date:
                    try:
                        # Parse: "Fri, 18 Jul 2025 06:25:48 GMT"
                        dt = datetime.strptime(published_date, '%a, %d %b %Y %H:%M:%S %Z')
                        formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        formatted_date = published_date
                else:
                    formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                article = {
                    'title': current_article.get('title', ''),
                    'url': current_article.get('url', ''),
                    'publisher': current_article.get('publisher', 'NewsNow'),
                    'published_date': formatted_date,
                    'summary': current_article.get('summary', ''),
                    'thumbnail': current_article.get('thumbnail', ''),
                    'language': 'en',
                    'category': 'business',
                    'full_content': current_article.get('content', '')
                }
                
                articles.append(article)
                logger.info(f"Parsed article: {article['title'][:50]}...")
            
            # Try to parse multiple articles from the response
            # This is a simplified approach - in reality you'd need to group by news index
            articles_data = self.extract_multiple_articles(response_text)
            if articles_data:
                articles = articles_data
                
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            
        logger.info(f"Total articles parsed: {len(articles)}")
        return articles
    
    def extract_multiple_articles(self, response_text: str) -> List[Dict[str, Any]]:
        """Extract multiple articles from response"""
        articles = []
        articles_data = {}
        
        try:
            lines = response_text.strip().split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or ':' not in line:
                    continue
                
                if line.startswith('news:'):
                    parts = line.split(':', 3)
                    if len(parts) >= 3:
                        news_index = parts[1]
                        field_name = parts[2]
                        field_value = parts[3] if len(parts) > 3 else ""
                        
                        # Clean up the value
                        if field_value.startswith('"') and field_value.endswith('"'):
                            field_value = field_value[1:-1]
                        
                        # Initialize article if not exists
                        if news_index not in articles_data:
                            articles_data[news_index] = {}
                        
                        # Store field
                        if field_name == 'title':
                            articles_data[news_index]['title'] = field_value
                        elif field_name == 'url':
                            articles_data[news_index]['url'] = field_value
                        elif field_name == 'top_image':
                            articles_data[news_index]['thumbnail'] = field_value
                        elif field_name == 'date':
                            articles_data[news_index]['date'] = field_value
                        elif field_name == 'short_description':
                            articles_data[news_index]['summary'] = field_value
                        elif field_name == 'text':
                            articles_data[news_index]['content'] = field_value
                
                # Handle publisher fields
                elif 'publisher:title:' in line:
                    parts = line.split(':', 4)
                    if len(parts) >= 4:
                        news_index = parts[1]
                        field_value = parts[4] if len(parts) > 4 else ""
                        if field_value.startswith('"') and field_value.endswith('"'):
                            field_value = field_value[1:-1]
                        
                        if news_index not in articles_data:
                            articles_data[news_index] = {}
                        articles_data[news_index]['publisher'] = field_value
            
            # Convert to articles list
            for index, data in articles_data.items():
                if data.get('title') and data.get('url'):
                    # Convert date
                    published_date = data.get('date', '')
                    if published_date:
                        try:
                            dt = datetime.strptime(published_date, '%a, %d %b %Y %H:%M:%S %Z')
                            formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            formatted_date = published_date
                    else:
                        formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    article = {
                        'title': data.get('title', ''),
                        'url': data.get('url', ''),
                        'publisher': data.get('publisher', 'NewsNow'),
                        'published_date': formatted_date,
                        'summary': data.get('summary', ''),
                        'thumbnail': data.get('thumbnail', ''),
                        'language': 'en',
                        'category': 'business',
                        'full_content': data.get('content', '')
                    }
                    
                    articles.append(article)
            
        except Exception as e:
            logger.error(f"Error extracting articles: {e}")
            
        return articles
    
    def fetch_and_store_news(self):
        """Fetch news with ultra-conservative rate limiting"""
        try:
            logger.info("🚀 Starting CONSERVATIVE news fetch process")
            
            total_articles = 0
            fetch_timestamp = int(time.time())
            max_pages = int(os.getenv('MAX_PAGES', '1'))  # Default to just 1 page
            
            # Only try ONE page to avoid rate limits
            for page in range(1, max_pages + 1):  # Start from page 1
                try:
                    logger.info(f"📄 Attempting to fetch page {page} of {max_pages}")
                    
                    # Fetch page with long waits
                    response_text = self.fetch_news_page(page)
                    if not response_text:
                        logger.warning(f"❌ Failed to get response from page {page}")
                        # Wait 30 seconds before trying next page
                        logger.info("⏳ Waiting 30s before continuing...")
                        time.sleep(30)
                        continue
                    
                    # Parse articles
                    articles = self.parse_response(response_text)
                    if not articles:
                        logger.warning(f"❌ No articles parsed from page {page}")
                        logger.debug(f"Response preview: {response_text[:200]}...")
                        # Still wait to avoid hitting API again quickly
                        time.sleep(10)
                        continue
                    
                    # Store articles
                    page_fetch_order = fetch_timestamp + (page * 1000)
                    inserted_count = db.bulk_insert_articles(articles, page_fetch_order)
                    
                    total_articles += inserted_count
                    logger.info(f"✅ Page {page}: Successfully stored {inserted_count} articles")
                    
                    # Wait 60 seconds between pages if doing multiple
                    if page < max_pages:
                        logger.info("⏳ Waiting 60s before next page...")
                        time.sleep(60)
                        
                except Exception as e:
                    logger.error(f"❌ Error processing page {page}: {e}")
                    # Wait 30 seconds even on error
                    time.sleep(30)
                    continue
            
            logger.info(f"🎉 Total articles stored: {total_articles}")
            
            # Only cleanup if we actually got some articles
            if total_articles > 0:
                self.cleanup_database()
            
        except Exception as e:
            logger.error(f"❌ Error in fetch_and_store_news: {e}")
    
    def cleanup_database(self):
        """Clean up database"""
        try:
            old_deleted = db.cleanup_old_articles()
            excess_deleted = db.cleanup_excess_articles()
            
            total_deleted = old_deleted + excess_deleted
            if total_deleted > 0:
                logger.info(f"Cleaned up {total_deleted} articles")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
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
        """Manual fetch trigger"""
        try:
            logger.info("Manual fetch triggered")
            self.fetch_and_store_news()
            return {'status': 'success', 'message': 'Fetch completed'}
        except Exception as e:
            logger.error(f"Manual fetch failed: {e}")
            return {'status': 'error', 'message': str(e)}

# Global instance
news_fetcher = NewsFetcher()
