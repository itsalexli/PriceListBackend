from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import tempfile
import asyncio
from typing import Dict, Any
import traceback
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import optimized scraper
from scraper import OptimizedPriceScraper
from agent import call_gemini_api

app = FastAPI(title="Optimized Price Scraper API - Gemini Powered", version="2.1.0")

# Configure CORS origins from environment or use defaults
cors_origins = os.getenv("CORS_ORIGINS", "").split(",") if os.getenv("CORS_ORIGINS") else ["*"]

# Add CORS middleware 
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "HEAD", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin", "X-Requested-With"],
    expose_headers=["Content-Disposition"]
)

class ScrapeRequest(BaseModel):
    url: str
    max_pages: int = 50
    max_workers: int = 5

class ScrapeResponse(BaseModel):
    success: bool
    message: str
    data: Dict[Any, Any] = None
    formatted_prices: str = None
    processing_time: float = None

@app.get("/")
@app.head("/")  # Add HEAD method support for health checks
async def root():
    return {
        "message": "Optimized Price Scraper API is running", 
        "status": "healthy",
        "version": "2.1.0",
        "cors_enabled": True,
        "endpoints": ["/", "/scrape", "/health", "/test"]
    }

@app.get("/test")
async def test_connection():
    """Simple test endpoint to verify frontend-backend connectivity"""
    return {
        "success": True,
        "message": "Backend connection successful!",
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "cors_headers": "enabled"
    }

@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_website(request: ScrapeRequest, http_request: Request):
    # Log the request for debugging
    print(f"🌐 Received scrape request from: {http_request.client.host if http_request.client else 'unknown'}")
    print(f"📝 Request data: URL={request.url}, max_pages={request.max_pages}")
    
    temp_filename = None
    start_time = time.time()
    
    try:
        print(f"🚀 Starting optimized scrape for: {request.url}")
        print(f"📊 Using Gemini AI for price analysis and categorization")
        
        # Enhanced URL validation
        if not request.url.startswith(('http://', 'https://')):
            raise HTTPException(
                status_code=400, 
                detail="URL must start with http:// or https://"
            )

        # Validate numerical parameters
        if request.max_pages < 1 or request.max_pages > 200:
            raise HTTPException(
                status_code=400, 
                detail="max_pages must be between 1 and 200"
            )
        if request.max_workers < 1 or request.max_workers > 10:
            raise HTTPException(
                status_code=400, 
                detail="max_workers must be between 1 and 10"
            )

        # Initialize scraper with safe defaults
        scraper = OptimizedPriceScraper(
            max_pages=min(request.max_pages, 100),  # Additional safety cap
            delay=1.5,  # Slightly longer delay to be more respectful
            max_workers=min(request.max_workers, 5)  # Conservative worker limit
        )

        print(f"🔧 Scraping with {scraper.max_workers} parallel workers...")

        # Scrape the website with timeout protection - FIX: Call correct method
        try:
            results = await asyncio.wait_for(
                asyncio.to_thread(scraper.scrape_website_parallel, request.url),
                timeout=300  # 5 minute timeout
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail="Scraping operation timed out (5 minute limit)"
            )

        scrape_time = time.time()
        print(f"✅ Scraping completed in {scrape_time - start_time:.2f}s")
        
        # Enhanced empty results handling
        total_prices = len(results.get('all_prices', []))
        total_pdfs = len(results.get('downloaded_pdfs', []))
        
        if total_prices == 0 and total_pdfs == 0:
            print("⚠️ No pricing data found")
            return ScrapeResponse(
                success=False,
                message="No pricing information found on the website.",
                data={
                    "scrape_results": {
                        "total_pages_scraped": results.get('total_pages_scraped', 0),
                        "pages_with_prices": 0,
                        "unique_prices_found": 0,
                        "pdfs_downloaded": 0,
                        "suggestions": [
                            "Try a different URL or website",
                            "Check for 'General Price List' or 'GPL' links",
                            "The website may require JavaScript or block automated access"
                        ]
                    }
                },
                processing_time=scrape_time - start_time
            )

        # Create temporary file for excerpts - FIX: Use proper temporary file handling
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8')
        temp_filename = temp_file.name
        temp_file.close()  # Close the file so extract_price_excerpts_fast can write to it

        # Extract price excerpts
        excerpt_file = scraper.extract_price_excerpts_fast(results, temp_filename)
        if not excerpt_file:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract price excerpts"
            )

        excerpt_time = time.time()
        print(f"✅ Excerpt extraction completed in {excerpt_time - scrape_time:.2f}s")

        # Get Gemini API key securely
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=500,
                detail="GEMINI_API_KEY environment variable not set. Please configure your API key."
            )
        
        print(f"🔑 API key configured: {api_key[:20]}...")

        # Call Gemini API with error handling
        print("🤖 Processing with Gemini AI...")
        try:
            formatted_response = await asyncio.wait_for(
                asyncio.to_thread(call_gemini_api, excerpt_file, api_key),
                timeout=120  # 2 minute timeout for AI processing
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail="AI processing timed out (2 minute limit)"
            )
        except Exception as api_error:
            print(f"❌ Gemini API error: {str(api_error)}")
            raise HTTPException(
                status_code=500,
                detail=f"AI processing error: {str(api_error)}"
            )

        ai_time = time.time()
        print(f"✅ Gemini AI processing completed in {ai_time - excerpt_time:.2f}s")
        
        if isinstance(formatted_response, dict) and "error" in formatted_response:
            print(f"❌ Gemini AI processing failed: {formatted_response['error']}")
            raise HTTPException(
                status_code=500, 
                detail=f"AI processing failed: {formatted_response['error']}"
            )

        # Process formatted response
        formatted_lines = []
        if isinstance(formatted_response, str):
            formatted_lines = [
                line.strip() for line in formatted_response.split('\n') 
                if line.strip() and ':' in line and '$' in line
            ]

        total_time = time.time() - start_time

        # Prepare response data
        response_data = {
            "scrape_results": {
                "total_pages_scraped": results['total_pages_scraped'],
                "pages_with_prices": len(results['pages_with_prices']),
                "unique_prices_found": len(set(results['all_prices'])),
                "pdfs_downloaded": len(results['downloaded_pdfs']),
                "formatted_items_count": len(formatted_lines),
                "sample_prices": sorted(set(results['all_prices']))[:20]
            },
            "ai_processing": {
                "provider": "Google Gemini Pro",
                "items_processed": len(formatted_lines),
                "processing_time": round(ai_time - excerpt_time, 2)
            },
            "performance": {
                "total_time": round(total_time, 2),
                "scrape_time": round(scrape_time - start_time, 2),
                "excerpt_time": round(excerpt_time - scrape_time, 2),
                "workers_used": scraper.max_workers
            }
        }

        return ScrapeResponse(
            success=True,
            message=(
                f"Successfully processed {len(formatted_lines)} items "
                f"in {total_time:.2f} seconds"
            ),
            data=response_data,
            formatted_prices=formatted_response,
            processing_time=total_time
        )

    except HTTPException:
        raise
    except Exception as e:
        error_time = time.time() - start_time
        print(f"❌ Error after {error_time:.2f}s: {str(e)}")
        print(f"🔍 Traceback: {traceback.format_exc()}")
        
        error_message = "An unexpected error occurred during processing"
        if "timeout" in str(e).lower():
            error_message = "Processing timed out - please try with a smaller scope"
        elif "connection" in str(e).lower():
            error_message = "Connection error - please check the URL and try again"
        
        raise HTTPException(
            status_code=500, 
            detail=error_message
        )
    
    finally:
        # Clean up temporary file
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.unlink(temp_filename)
                print("🧹 Cleaned up temporary file")
            except Exception as e:
                print(f"⚠️ Warning: Could not delete temp file: {e}")

@app.get("/health")
async def health_check():
    gemini_key_configured = bool(os.getenv("GEMINI_API_KEY"))
    return {
        "status": "healthy", 
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "version": "2.1.0",
        "environment": {
            "gemini_api_configured": gemini_key_configured,
            "cors_origins": os.getenv("CORS_ORIGINS", "all"),
            "port": os.getenv("PORT", "8000")
        },
        "dependencies": ["Google Gemini API", "FastAPI", "BeautifulSoup", "PyPDF2"]
    }

if __name__ == "__main__":
    print("🚀 Starting Gemini-powered Funeral Price Scraper API...")
    print("📊 API Version: 2.1.0")
    
    import uvicorn
    port = int(os.getenv("PORT", 8000))  # Use PORT env var or default to 8000
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info",
        timeout_keep_alive=30
    )