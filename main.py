from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import os
from datetime import datetime
from dotenv import load_dotenv
from database import db
from news_fetcher import news_fetcher

load_dotenv()

# Pydantic models
class ArticleResponse(BaseModel):
    id: int
    title: str
    url: str
    publisher: str
    published_date: str
    summary: str
    thumbnail: str
    language: str
    category: str
    created_at: str
    updated_at: str

class ArticleDetailResponse(ArticleResponse):
    full_content: str

class PaginatedResponse(BaseModel):
    articles: List[ArticleResponse]
    total_count: int
    page: int
    limit: int
    total_pages: int

class StatsResponse(BaseModel):
    total_articles: int
    latest_article_date: Optional[str]
    oldest_article_date: Optional[str]
    recent_api_calls_24h: int
    retention_days: int
    max_articles: int

# Create FastAPI app
app = FastAPI(
    title="NewsNow Business API",
    description="Indian Business News API powered by NewsNow",
    version="1.0.0"
)

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100

@app.on_event("startup")
async def startup_event():
    """Start the news fetcher"""
    try:
        print("Starting NewsNow API...")
        news_fetcher.start_scheduler()
        print("NewsNow API started successfully")
    except Exception as e:
        print(f"Error during startup: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the news fetcher"""
    try:
        print("Shutting down NewsNow API...")
        news_fetcher.stop_scheduler()
        print("NewsNow API shut down successfully")
    except Exception as e:
        print(f"Error during shutdown: {e}")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "NewsNow Indian Business News API",
        "version": "1.0.0",
        "description": "Indian Business News from NewsNow API",
        "endpoints": {
            "articles": "/articles - Get articles with pagination",
            "article_detail": "/articles/{id} - Get specific article",
            "latest": "/latest - Get latest articles",
            "stats": "/stats - Get database statistics",
            "manual_fetch": "/admin/fetch - Manually fetch news"
        }
    }

@app.get("/health")
async def health_check():
    """Health check"""
    try:
        stats = db.get_database_stats()
        scheduler_status = news_fetcher.get_scheduler_status()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected" if stats else "error",
            "scheduler": "running" if scheduler_status.get('scheduler_running', False) else "stopped",
            "total_articles": stats.get('total_articles', 0)
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.get("/articles", response_model=PaginatedResponse)
async def get_articles(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Items per page"),
    search: Optional[str] = Query(None, description="Search in title and summary"),
    date_from: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """Get paginated articles"""
    try:
        result = db.get_articles(
            page=page,
            limit=limit,
            search=search,
            date_from=date_from,
            date_to=date_to
        )
        return PaginatedResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/articles/{article_id}", response_model=ArticleDetailResponse)
async def get_article(article_id: int = Path(..., ge=1, description="Article ID")):
    """Get specific article by ID"""
    try:
        article = db.get_article_by_id(article_id)
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        return ArticleDetailResponse(**article)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/latest", response_model=List[ArticleResponse])
async def get_latest_articles(
    limit: int = Query(10, ge=1, le=50, description="Number of latest articles")
):
    """Get latest articles"""
    try:
        result = db.get_articles(page=1, limit=limit)
        return result['articles']
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Get database statistics"""
    try:
        stats = db.get_database_stats()
        if not stats:
            raise HTTPException(status_code=500, detail="Unable to retrieve statistics")
        return StatsResponse(**stats)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/admin/fetch")
async def manual_fetch():
    """Manually trigger news fetch"""
    try:
        result = news_fetcher.manual_fetch()
        if result['status'] == 'error':
            raise HTTPException(status_code=500, detail=result['message'])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/admin/scheduler")
async def get_scheduler_status():
    """Get scheduler status"""
    try:
        status = news_fetcher.get_scheduler_status()
        if 'error' in status:
            raise HTTPException(status_code=500, detail=status['error'])
        return status
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/admin/cleanup")
async def manual_cleanup():
    """Manually trigger database cleanup"""
    try:
        old_deleted = db.cleanup_old_articles()
        excess_deleted = db.cleanup_excess_articles()
        
        return {
            "status": "success",
            "message": "Cleanup completed",
            "deleted_articles": {
                "old_articles": old_deleted,
                "excess_articles": excess_deleted,
                "total_deleted": old_deleted + excess_deleted
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/search", response_model=PaginatedResponse)
async def search_articles(
    q: str = Query(..., description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Items per page")
):
    """Search articles"""
    try:
        result = db.get_articles(page=page, limit=limit, search=q)
        return PaginatedResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# Run the application
if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False
    )
