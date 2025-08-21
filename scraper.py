import requests
import requests.adapters
import re
import os
import time
from urllib.parse import urljoin, urlparse, urlunparse
from bs4 import BeautifulSoup
from collections import deque
import PyPDF2
# pdfplumber import moved to function level to handle optional dependency
import io
from typing import Set, List, Dict, Tuple
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import warnings
import random

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='urllib3 v2 only supports OpenSSL 1.1.1+')

from agent import call_gemini_api


class OptimizedPriceScraper:
    def __init__(self, max_pages=50, delay=0, max_workers=6):
        self.session = requests.Session()
        
        # Configure session for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=2
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Enhanced headers to avoid bot detection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Set random user agent and additional headers
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        self.max_pages = max_pages
        self.delay = delay
        self.max_workers = min(max_workers, 8)  # Allow up to 8 workers
        self.visited_urls = set()
        self.pdf_folder = "downloaded_pdfs"
        
        # ENHANCED: More comprehensive price patterns - compiled once for speed
        self.price_patterns = [
            # Standard formats
            re.compile(r'\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?', re.IGNORECASE),
            re.compile(r'\$\s*\d+(?:\.\d{1,2})?', re.IGNORECASE),
            re.compile(r'USD\s*\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?', re.IGNORECASE),
            re.compile(r'\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\s*USD', re.IGNORECASE),
            re.compile(r'Price:?\s*\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?', re.IGNORECASE),
            re.compile(r'Cost:?\s*\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?', re.IGNORECASE),
            re.compile(r'\b\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\s*dollars?\b', re.IGNORECASE),
            # Table formats (common in GPLs)
            re.compile(r'(?<=\s)\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?=\s|$)', re.IGNORECASE),
            re.compile(r'^\d{1,3}(?:,\d{3})*(?:\.\d{2})?$', re.IGNORECASE),
        ]
        
        # Pre-compile patterns for better performance
        self.html_tag_pattern = re.compile(r'<[^>]+>')
        self.whitespace_pattern = re.compile(r'\s+')
        self.technical_patterns = re.compile(
            r'{\s*["\'][\w-]+["\']:\s*["\']|'
            r'margin-top|padding|border-width|background-color|'
            r'slug|ver|options|elements|settings|'
            r'rgba?\(\d+,\d+,\d+|'
            r'px|em|rem|%"|'
            r'["\'][\w-]+["\']:\s*["\'][\w-]+["\']|'
            r'[^\x20-\x7E]{5,}|'
            r'����|�{3,}',
            re.IGNORECASE
        )
        
        self.pdf_signatures = {}
        self.page_signatures = {}
        self.lock = threading.Lock()
        self.request_count = 0
        
        os.makedirs(self.pdf_folder, exist_ok=True)

    def get_random_delay(self):
        """Get a random delay between requests"""
        return 0  # No delay

    def make_request(self, url, **kwargs):
        """Make a request with retry logic and bot detection avoidance"""
        max_retries = 2  # Reduced retries for speed
        
        for attempt in range(max_retries):
            try:
                # Rotate user agent less frequently for speed
                if self.request_count % 25 == 0:
                    self.session.headers['User-Agent'] = random.choice(self.user_agents)
                
                self.request_count += 1
                
                # Reduced timeout for faster processing
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = 8
                
                response = self.session.get(url, **kwargs)
                
                if response.status_code == 403:
                    print(f"⚠️ 403 Forbidden for {url} - trying with different headers")
                    minimal_session = requests.Session()
                    minimal_session.headers.update({
                        'User-Agent': random.choice(self.user_agents),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    })
                    response = minimal_session.get(url, timeout=8)
                
                elif response.status_code == 429:
                    # Rate limited - minimal wait
                    if attempt < max_retries - 1:
                        print(f"⚠️ Rate limited, retrying immediately...")
                        continue
                
                elif response.status_code in [502, 503, 504]:
                    if attempt < max_retries - 1:
                        print(f"⚠️ Server error {response.status_code}, retrying immediately...")
                        continue
                
                response.raise_for_status()
                return response
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⚠️ Request failed ({e}), retrying immediately...")
                    continue
                else:
                    raise e
        
        raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

    def is_valid_url(self, url: str, base_domain: str) -> bool:
        """Optimized URL validation"""
        try:
            parsed = urlparse(url)
            base_parsed = urlparse(base_domain)
            
            if parsed.netloc and parsed.netloc != base_parsed.netloc:
                return False
                
            path_lower = parsed.path.lower()
            skip_exts = {'.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.ico', '.svg', '.woff', '.woff2', '.ttf', '.eot'}
            return not any(path_lower.endswith(ext) for ext in skip_exts)
        except:
            return False

    def normalize_url(self, url: str) -> str:
        """Simplified URL normalization"""
        try:
            parsed = urlparse(url)
            return urlunparse(parsed._replace(fragment="", query="")).rstrip('/')
        except:
            return url

    def find_prices_in_text(self, text: str) -> List[str]:
        """Enhanced price extraction with multiple patterns"""
        if not text:
            return []
        
        all_matches = []
        
        for pattern in self.price_patterns:
            matches = pattern.findall(text)
            all_matches.extend(matches)
        
        cleaned_prices = []
        for price in all_matches:
            clean_price = re.sub(r'[^\d$.,]', '', price.strip())
            if clean_price and any(c.isdigit() for c in clean_price):
                if not clean_price.startswith('$'):
                    clean_price = '$' + clean_price
                cleaned_prices.append(clean_price)
        
        return list(dict.fromkeys(cleaned_prices))

    def is_readable_text(self, text: str) -> bool:
        """Enhanced readability check"""
        if not text or len(text.strip()) < 5:
            return False
        if '\x00' in text or text.count('�') > len(text) * 0.2:
            return False
        printable_chars = sum(1 for c in text if c.isprintable() or c.isspace())
        return (printable_chars / len(text)) > 0.6

    def extract_text_from_pdf_pypdf2(self, pdf_content: bytes) -> str:
        """PyPDF2 extraction method"""
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            
            if pdf_reader.is_encrypted:
                try:
                    pdf_reader.decrypt("")
                except:
                    return ""
            
            text_parts = []
            
            for page in pdf_reader.pages:
                try:
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        text_parts.append(page_text)
                except:
                    continue
            
            return "\n".join(text_parts)
            
        except Exception as e:
            print(f"PyPDF2 extraction failed: {e}")
            return ""

    def extract_gpl_format_prices(self, text: str) -> List[Tuple[str, List[str]]]:
        """
        Special extraction for General Price List (GPL) formatted documents.
        These often have service descriptions followed by prices in a tabular format.
        """
        price_entries = []
        
        # Common GPL patterns
        gpl_patterns = [
            # Service name followed by dots and price
            re.compile(r'^(.+?)\.{2,}\s*(\$?\s*[\d,]+(?:\.\d{2})?)\s*$', re.MULTILINE),
            # Service name with price at end of line
            re.compile(r'^(.+?)\s+(\$\s*[\d,]+(?:\.\d{2})?)\s*$', re.MULTILINE),
            # Table format with | separators
            re.compile(r'^([^|]+)\s*\|\s*.*?\|\s*(\$?\s*[\d,]+(?:\.\d{2})?)\s*$', re.MULTILINE),
            # Indented price format
            re.compile(r'^(.+?)\s{3,}(\$?\s*[\d,]+(?:\.\d{2})?)\s*$', re.MULTILINE),
        ]
        
        for pattern in gpl_patterns:
            matches = pattern.findall(text)
            for match in matches:
                if len(match) >= 2:
                    service = match[0].strip()
                    price = match[1].strip()
                    if not price.startswith('$'):
                        price = '$' + price
                    full_line = f"{service}: {price}"
                    price_entries.append((full_line, [price]))
        
        return price_entries

    def extract_text_from_pdf_pdfplumber(self, pdf_content: bytes) -> str:
        """pdfplumber extraction method - better for tables"""
        try:
            import pdfplumber
            
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                text_parts = []
                
                for page in pdf.pages:
                    try:
                        # Extract text
                        page_text = page.extract_text()
                        if page_text and page_text.strip():
                            text_parts.append(page_text)
                        
                        # ENHANCED: Also extract tables specifically
                        tables = page.extract_tables()
                        for table in tables:
                            if table:
                                # Convert table to text format
                                for row in table:
                                    if row:
                                        row_text = ' | '.join([str(cell) if cell else '' for cell in row])
                                        text_parts.append(row_text)
                    except:
                        continue
                
                return "\n".join(text_parts)
                
        except ImportError:
            print("pdfplumber not installed, falling back to PyPDF2")
            return ""
        except Exception as e:
            print(f"pdfplumber extraction failed: {e}")
            return ""

    def extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Enhanced PDF text extraction with multiple methods"""
        text = self.extract_text_from_pdf_pdfplumber(pdf_content)
        
        if not text or len(text.strip()) < 50:
            print("Trying PyPDF2 extraction...")
            text = self.extract_text_from_pdf_pypdf2(pdf_content)
        
        if text and self.is_readable_text(text):
            return text
        
        print("PDF text extraction failed or text not readable")
        return ""

    def extract_price_lines_from_pdf(self, pdf_text: str) -> List[Tuple[str, List[str]]]:
        """
        ENHANCED: Extract complete lines containing prices from PDF text.
        Returns list of (line_text, prices_found) tuples.
        """
        if not pdf_text:
            return []
        
        price_lines = []
        
        # Split by newlines to preserve table structure
        lines = pdf_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or len(line) < 10:
                continue
            
            # Find all prices in this line
            prices = self.find_prices_in_text(line)
            
            if prices:
                # Clean the line but keep it complete
                clean_line = re.sub(r'\s+', ' ', line).strip()
                
                # Skip purely technical lines
                if not self.technical_patterns.search(clean_line[:100]):
                    price_lines.append((clean_line, prices))
        
        return price_lines

    def clean_text_fast(self, text: str) -> str:
        """Fast text cleaning"""
        if not text:
            return ""
        text = self.html_tag_pattern.sub(' ', text)
        text = self.whitespace_pattern.sub(' ', text)
        return text.strip()

    def scrape_page_fast(self, url: str) -> Dict:
        """Optimized page scraping with better error handling"""
        try:
            print(f"📄 Scraping: {url}")
            response = self.make_request(url)
            
            # Use faster parser for speed, fallback to html.parser
            try:
                soup = BeautifulSoup(response.text, 'lxml')
            except:
                soup = BeautifulSoup(response.text, 'html.parser')
            
            for element in soup(['script', 'style', 'meta', 'link']):
                element.decompose()
            
            page_text = self.clean_text_fast(soup.get_text())
            
            prices = self.find_prices_in_text(page_text)
            if prices:
                print(f"✓ Found {len(prices)} prices on {url}")
            
            links = [urljoin(url, a.get('href', '')) 
                    for a in soup.find_all('a', href=True)]
            
            pdf_links = [link for link in links if link.lower().endswith('.pdf')]
            if pdf_links:
                print(f"📎 Found {len(pdf_links)} PDF links")
            
            return {
                'url': url,
                'prices': prices,
                'links': links,
                'pdf_links': pdf_links,
                'title': soup.title.string if soup.title else 'No title',
                'text': page_text
            }
            
        except Exception as e:
            print(f"✗ Error scraping {url}: {e}")
            return {
                'url': url, 'prices': [], 'links': [], 
                'pdf_links': [], 'title': 'Error', 'text': ''
            }

    def process_pdf_fast(self, pdf_url: str, source_url: str) -> Dict:
        """Enhanced PDF processing with complete extraction including GPL format"""
        try:
            print(f"📄 Processing PDF: {pdf_url}")
            response = self.make_request(pdf_url, timeout=20)  # Reduced timeout
            
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.lower().endswith('.pdf'):
                print(f"Not a PDF: {content_type}")
                return None
            
            print(f"🔍 Extracting text from PDF...")
            pdf_text = self.extract_text_from_pdf(response.content)
            
            if not pdf_text:
                print("No text extracted from PDF")
                return None
            
            print(f"✓ Extracted {len(pdf_text)} characters from PDF")
            
            # Check if this looks like a GPL document
            is_gpl = any(term in pdf_text.upper() for term in ['GENERAL PRICE LIST', 'GPL', 'PRICE LIST', 'FUNERAL PRICES'])
            
            price_lines = []
            
            if is_gpl:
                print("📋 Detected GPL format - using specialized extraction")
                # Use GPL-specific extraction
                gpl_entries = self.extract_gpl_format_prices(pdf_text)
                price_lines.extend(gpl_entries)
            
            # Also use standard extraction for any missed items
            standard_lines = self.extract_price_lines_from_pdf(pdf_text)
            price_lines.extend(standard_lines)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_price_lines = []
            for line, prices in price_lines:
                line_hash = hashlib.md5(line.encode()).hexdigest()[:16]
                if line_hash not in seen:
                    seen.add(line_hash)
                    unique_price_lines.append((line, prices))
            
            all_prices = []
            for line, prices in unique_price_lines:
                all_prices.extend(prices)
            
            print(f"✓ Found {len(unique_price_lines)} unique lines with prices, total {len(all_prices)} prices")
            
            content_signature = hashlib.md5(pdf_text[:1000].encode()).hexdigest()
            
            with self.lock:
                if content_signature in self.pdf_signatures:
                    print("Duplicate PDF content")
                    return None
                self.pdf_signatures[content_signature] = True
            
            parsed_url = urlparse(pdf_url)
            filename = os.path.basename(parsed_url.path)
            if not filename or not filename.endswith('.pdf'):
                filename = f"doc_{int(time.time())}.pdf"
            
            filename = re.sub(r'[^\w\-_\.]', '_', filename)
            
            try:
                filepath = os.path.join(self.pdf_folder, filename)
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"✓ Downloaded PDF: {filename}")
            except Exception as e:
                print(f"✗ Failed to save PDF {filename}: {e}")
                return None
            
            return {
                'pdf_url': pdf_url,
                'source_page': source_url,
                'filename': filename,
                'prices': all_prices,
                'price_lines': unique_price_lines,  # Complete lines with context
                'text': pdf_text,
                'is_gpl': is_gpl
            }
            
        except Exception as e:
            print(f"✗ Error processing PDF {pdf_url}: {e}")
            return None

    def scrape_website_parallel(self, start_url: str) -> Dict:
        """Main scraping function with parallel processing"""
        print(f"🚀 Starting optimized scrape of: {start_url}")
        print(f"📊 Scraping with {self.max_workers} parallel workers...")
        
        results = {
            'start_url': start_url,
            'pages_with_prices': [],
            'all_prices': [],
            'downloaded_pdfs': [],
            'total_pages_scraped': 0,
            'page_texts': {},
            'pdf_texts': {},
            'pdf_price_lines': {}  # ENHANCED: Store complete price lines from PDFs
        }
        
        base_domain = start_url
        queue = deque([start_url])
        pending_pdfs = []
        
        try:
            print("🔍 Testing initial connection...")
            test_response = self.make_request(start_url)
            print(f"✓ Initial connection successful (Status: {test_response.status_code})")
        except Exception as e:
            print(f"✗ Failed to connect to {start_url}: {e}")
            return results
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while queue and len(self.visited_urls) < self.max_pages:
                batch = []
                # Larger batch size for better efficiency
                while queue and len(batch) < self.max_workers * 2:
                    url = self.normalize_url(queue.popleft())
                    if url not in self.visited_urls and self.is_valid_url(url, base_domain):
                        self.visited_urls.add(url)
                        batch.append(url)
                
                if not batch:
                    break
                
                print(f"🔄 Processing batch of {len(batch)} URLs...")
                
                future_to_url = {
                    executor.submit(self.scrape_page_fast, url): url 
                    for url in batch
                }
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        page_data = future.result()
                        results['total_pages_scraped'] += 1
                        
                        if page_data['prices']:
                            price_sig = hashlib.md5('|'.join(sorted(page_data['prices'])).encode()).hexdigest()
                            
                            with self.lock:
                                if price_sig not in self.page_signatures:
                                    self.page_signatures[price_sig] = True
                                    
                                    page_info = {
                                        'url': url,
                                        'title': page_data['title'],
                                        'prices': page_data['prices']
                                    }
                                    results['pages_with_prices'].append(page_info)
                                    results['all_prices'].extend(page_data['prices'])
                                    results['page_texts'][url] = page_data['text']
                                    
                                    print(f"✓ Found prices on: {url}")
                        
                        for pdf_url in page_data['pdf_links']:
                            pending_pdfs.append((pdf_url, url))
                        
                        for link in page_data['links']:
                            norm_link = self.normalize_url(link)
                            if (norm_link not in self.visited_urls and 
                                self.is_valid_url(norm_link, base_domain) and
                                len(self.visited_urls) < self.max_pages):
                                queue.append(norm_link)
                    
                    except Exception as e:
                        print(f"✗ Error processing {url}: {e}")
                
                # Removed batch delay for faster processing
        
        if pending_pdfs:
            print(f"📎 Processing {len(pending_pdfs)} PDFs...")
            # Increased PDF workers for better parallelism
            with ThreadPoolExecutor(max_workers=min(4, self.max_workers)) as pdf_executor:
                pdf_futures = {
                    pdf_executor.submit(self.process_pdf_fast, pdf_url, source_url): (pdf_url, source_url)
                    for pdf_url, source_url in pending_pdfs[:25]  # Process more PDFs
                }

                for future in as_completed(pdf_futures):
                    pdf_url, source_url = pdf_futures[future]
                    try:
                        pdf_data = future.result()
                        if pdf_data:
                            results['downloaded_pdfs'].append({
                                'pdf_url': pdf_data['pdf_url'],
                                'source_page': pdf_data['source_page'],
                                'filename': pdf_data['filename'],
                                'prices': pdf_data['prices']
                            })
                            results['pdf_texts'][pdf_data['pdf_url']] = pdf_data['text']
                            results['pdf_price_lines'][pdf_data['pdf_url']] = pdf_data.get('price_lines', [])
                            results['all_prices'].extend(pdf_data['prices'])
                            
                            if pdf_data['prices']:
                                print(f"✓ PDF with prices: {pdf_data['filename']} - {len(pdf_data['prices'])} prices")
                            else:
                                print(f"✓ PDF processed (no prices): {pdf_data['filename']}")
                                
                    except Exception as e:
                        print(f"✗ Error with PDF {pdf_url}: {e}")
        
        return results

    def extract_price_excerpts_fast(self, results: Dict, output_filename: str = "price_excerpts.txt") -> str:
        """
        ENHANCED: Extract ALL price lines, especially from PDFs with complete context
        """
        print(f"📝 Extracting ALL price excerpts to {output_filename}...")
        
        all_excerpts = []
        seen_hashes = set()
        
        # Process page texts - extract lines with prices
        for url, text in results['page_texts'].items():
            if not text:
                continue
            
            # Split into lines first, then sentences if needed
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if len(line) < 20:
                    continue
                
                prices = self.find_prices_in_text(line)
                if prices:
                    excerpt_hash = hashlib.md5(line.encode()).hexdigest()[:16]
                    if excerpt_hash not in seen_hashes:
                        seen_hashes.add(excerpt_hash)
                        all_excerpts.append({
                            'source': url,
                            'text': line,
                            'prices': prices,
                            'type': 'webpage'
                        })
            
            # Also process sentences for better granularity
            sentences = re.split(r'[.!?]+', text)
            for sentence in sentences:
                sentence = sentence.strip()
                if len(sentence) < 20 or self.technical_patterns.search(sentence):
                    continue
                
                prices = self.find_prices_in_text(sentence)
                if prices:
                    excerpt_hash = hashlib.md5(sentence.encode()).hexdigest()[:16]
                    if excerpt_hash not in seen_hashes:
                        seen_hashes.add(excerpt_hash)
                        all_excerpts.append({
                            'source': url,
                            'text': sentence,
                            'prices': prices,
                            'type': 'webpage'
                        })
        
        # ENHANCED: Process PDF price lines - get ALL of them
        for pdf_url, price_lines in results.get('pdf_price_lines', {}).items():
            if not price_lines:
                continue
            
            print(f"📑 Processing {len(price_lines)} price lines from PDF: {pdf_url}")
            
            for line_text, prices in price_lines:
                # Don't truncate - include the full line
                excerpt_hash = hashlib.md5(line_text.encode()).hexdigest()[:16]
                if excerpt_hash not in seen_hashes:
                    seen_hashes.add(excerpt_hash)
                    all_excerpts.append({
                        'source': pdf_url,
                        'text': line_text,  # Full line, not truncated
                        'prices': prices,
                        'type': 'pdf'
                    })
        
        # Also process raw PDF text for any missed prices
        for pdf_url, text in results.get('pdf_texts', {}).items():
            if not text:
                continue
            
            # Split by lines to preserve table structure
            lines = text.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if len(line) < 10:
                    continue
                
                prices = self.find_prices_in_text(line)
                if prices:
                    # Get context from surrounding lines if available
                    context_lines = []
                    if i > 0:
                        context_lines.append(lines[i-1].strip())
                    context_lines.append(line)
                    if i < len(lines) - 1:
                        context_lines.append(lines[i+1].strip())
                    
                    context_text = ' | '.join([l for l in context_lines if l])
                    
                    excerpt_hash = hashlib.md5(context_text.encode()).hexdigest()[:16]
                    if excerpt_hash not in seen_hashes:
                        seen_hashes.add(excerpt_hash)
                        all_excerpts.append({
                            'source': pdf_url,
                            'text': context_text,
                            'prices': prices,
                            'type': 'pdf_context'
                        })
        
        # Write to file with ALL excerpts
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"COMPLETE PRICE EXCERPTS FROM: {results['start_url']}\n")
                f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total excerpts: {len(all_excerpts)}\n")
                f.write(f"Webpage excerpts: {sum(1 for e in all_excerpts if e['type'] == 'webpage')}\n")
                f.write(f"PDF excerpts: {sum(1 for e in all_excerpts if e['type'] in ['pdf', 'pdf_context'])}\n")
                f.write("=" * 80 + "\n\n")
                
                # Group by source for better organization
                excerpts_by_source = {}
                for excerpt in all_excerpts:
                    source = excerpt['source']
                    if source not in excerpts_by_source:
                        excerpts_by_source[source] = []
                    excerpts_by_source[source].append(excerpt)
                
                # Write excerpts grouped by source
                excerpt_num = 1
                for source, source_excerpts in excerpts_by_source.items():
                    f.write(f"\n{'='*60}\n")
                    f.write(f"SOURCE: {source}\n")
                    f.write(f"Type: {source_excerpts[0]['type'].upper()}\n")
                    f.write(f"Excerpts from this source: {len(source_excerpts)}\n")
                    f.write(f"{'='*60}\n\n")
                    
                    for excerpt in source_excerpts:
                        f.write(f"[{excerpt_num}] {excerpt['text']}\n")
                        f.write(f"PRICES FOUND: {', '.join(excerpt['prices'])}\n")
                        f.write("-" * 40 + "\n")
                        excerpt_num += 1
                    f.write("\n")
            
            print(f"✓ Wrote {len(all_excerpts)} COMPLETE excerpts to {output_filename}")
            
            # Print summary statistics
            print("\n" + "=" * 60)
            print("EXTRACTION SUMMARY:")
            print("=" * 60)
            print(f"Total price excerpts extracted: {len(all_excerpts)}")
            print(f"Unique prices found: {len(set([p for e in all_excerpts for p in e['prices']]))}")
            print(f"Sources processed: {len(excerpts_by_source)}")
            
            # Show sample of extracted content
            print("\nSAMPLE OF EXTRACTED CONTENT:")
            print("-" * 40)
            sample_count = min(5, len(all_excerpts))
            for i, excerpt in enumerate(all_excerpts[:sample_count], 1):
                print(f"{i}. {excerpt['text'][:100]}...")
                print(f"   Prices: {', '.join(excerpt['prices'][:3])}")
            
            # Print the complete file contents
            print("\n" + "=" * 80)
            print("COMPLETE TXT FILE CONTENTS:")
            print("=" * 80)
            try:
                with open(output_filename, 'r', encoding='utf-8') as f:
                    file_contents = f.read()
                    print(file_contents)
            except Exception as e:
                print(f"Error reading file for printing: {e}")
            print("=" * 80)
            print("END OF FILE CONTENTS")
            print("=" * 80 + "\n")
            
            return output_filename
            
        except Exception as e:
            print(f"✗ Error writing file: {e}")
            return None