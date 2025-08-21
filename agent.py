import requests
import json
import time

def call_gemini_api(filename, api_key, max_retries=3):
    """
    Enhanced Gemini API call with updated endpoint and better error handling
    """
    # Read the file contents
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            file_content = file.read()
    except Exception as e:
        return {"error": f"Failed to read file: {str(e)}"}

    # Updated Gemini API endpoint - using gemini-1.5-flash (recommended for most use cases)
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"

    # Headers
    headers = {"Content-Type": "application/json"}

    # Enhanced prompt for better price extraction
    prompt = f"""You are a data cleaning expert specializing in funeral home price lists. Analyze the text excerpts and produce a clean, deduplicated list of services and their prices.

Follow these rules strictly:
1. Format each entry as: `Item Name: $Price` (exactly this format).
2. Use the first price encountered when duplicates exist.
3. Ignore non-service/non-product text, technical content, and website navigation elements.
4. Only include items that are clearly funeral services, products, or merchandise.
5. Clean up item names to be professional and readable.
6. Ensure prices are in standard dollar format ($X,XXX.XX).
7. Group similar items but keep them as separate entries if they have different prices.
8. Output ONLY the clean list without explanations, summaries, or additional text.

Data to analyze:

{file_content}"""

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "temperature": 0.1,  # Lower temperature for more consistent results
            "maxOutputTokens": 3000,  # Increased for larger lists
            "topP": 0.8,
            "topK": 10
        }
    }

    # Retry logic
    for attempt in range(max_retries):
        try:
            print(f"Calling Gemini API (attempt {attempt + 1}/{max_retries})...")
            
            # Make the API call
            response = requests.post(api_url, headers=headers, json=payload, timeout=60)
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = (2 ** attempt) + 1  # Exponential backoff
                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            
            # Handle API key issues
            if response.status_code == 400:
                error_data = response.json() if response.content else {}
                if "API_KEY_INVALID" in str(error_data):
                    return {"error": "Invalid API key. Please check your GEMINI_API_KEY"}
                
            # Handle quota exceeded
            if response.status_code == 403:
                return {"error": "API quota exceeded or access denied. Check your Gemini API billing and permissions"}
                
            response.raise_for_status()

            # Parse the response
            response_data = response.json()

            # Extract the text response
            if 'candidates' in response_data and response_data['candidates']:
                candidate = response_data['candidates'][0]
                if 'content' in candidate and 'parts' in candidate['content']:
                    result = candidate['content']['parts'][0]['text']
                    
                    # Validate the result
                    if result and len(result.strip()) > 0:
                        print(f"✓ Successfully received {len(result)} characters from Gemini API")
                        return result
                    else:
                        print("⚠️ Empty response from Gemini API")
                        if attempt == max_retries - 1:
                            return {"error": "Empty response from Gemini API"}
                else:
                    print("⚠️ Unexpected response structure from Gemini API")
                    if attempt == max_retries - 1:
                        return {"error": "Invalid response structure from Gemini API"}
            else:
                print("⚠️ No candidates in Gemini API response")
                if attempt == max_retries - 1:
                    return {"error": "No valid response received from Gemini API"}

        except requests.exceptions.Timeout:
            print(f"⚠️ Request timeout (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return {"error": "Request timeout - Gemini API took too long to respond"}
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error: {str(e)}")
            if attempt == max_retries - 1:
                return {"error": f"API request failed: {str(e)}"}
            time.sleep(2)
            
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON decode error: {str(e)}")
            if attempt == max_retries - 1:
                return {"error": f"Invalid JSON response: {str(e)}"}
            time.sleep(2)
            
        except Exception as e:
            print(f"⚠️ Unexpected error: {str(e)}")
            if attempt == max_retries - 1:
                return {"error": f"Unexpected error: {str(e)}"}
            time.sleep(2)

    return {"error": f"Failed after {max_retries} attempts"}


def call_gemini_for_categorization(items_text, api_key, max_retries=3):
    """
    Call Gemini API specifically for categorizing funeral home items
    """
    # Updated API endpoint
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}

    prompt = f"""You are a funeral industry expert. Categorize these funeral services and products into logical groups. 

Create 6-8 meaningful categories that make sense for funeral planning. Common categories include:
- Professional Services
- Caskets & Containers  
- Cremation Services
- Burial Services
- Transportation
- Facility Usage
- Memorial Items
- Other Services

Always put 'Other Services' last. Return ONLY a valid JSON object where keys are category names and values are arrays of item names that belong to that category.

Items to categorize:

{items_text}

Return ONLY a JSON object with category names as keys and arrays of exact item names as values. Make sure 'Other Services' is the last category. Do not include any other text or explanation."""

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 2000,
            "topP": 0.8
        }
    }

    for attempt in range(max_retries):
        try:
            print(f"Calling Gemini API for categorization (attempt {attempt + 1}/{max_retries})...")
            
            response = requests.post(api_url, headers=headers, json=payload, timeout=45)
            
            if response.status_code == 429:
                wait_time = (2 ** attempt) + 1
                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            
            # Handle API key issues
            if response.status_code == 400:
                error_data = response.json() if response.content else {}
                if "API_KEY_INVALID" in str(error_data):
                    return {"error": "Invalid API key for categorization"}
                    
            response.raise_for_status()
            response_data = response.json()

            if 'candidates' in response_data and response_data['candidates']:
                candidate = response_data['candidates'][0]
                if 'content' in candidate and 'parts' in candidate['content']:
                    result = candidate['content']['parts'][0]['text'].strip()
                    
                    # Try to parse as JSON
                    try:
                        # Look for JSON in the response
                        if result.startswith('{') and result.endswith('}'):
                            categorization = json.loads(result)
                            return categorization
                        else:
                            # Try to extract JSON from the response
                            import re
                            json_match = re.search(r'\{.*\}', result, re.DOTALL)
                            if json_match:
                                categorization = json.loads(json_match.group())
                                return categorization
                    except json.JSONDecodeError:
                        print("⚠️ Could not parse JSON from categorization response")
                        
                    if attempt == max_retries - 1:
                        return {"error": "Could not parse JSON from categorization response"}
                        
        except Exception as e:
            print(f"⚠️ Categorization error: {str(e)}")
            if attempt == max_retries - 1:
                return {"error": f"Categorization failed: {str(e)}"}
            time.sleep(2)

    return {"error": f"Categorization failed after {max_retries} attempts"}


def test_api_key(api_key):
    """
    Test if the API key is valid with a simple request
    """
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{
            "parts": [{"text": "Hello, test message. Please respond with 'API key is working'."}]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 20
        }
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'candidates' in data and data['candidates']:
                return True, "API key is valid"
        elif response.status_code == 400:
            return False, "Invalid API key"
        elif response.status_code == 403:
            return False, "API key access denied or quota exceeded"
        else:
            return False, f"API test failed with status {response.status_code}"
            
    except Exception as e:
        return False, f"API test error: {str(e)}"
    
    return False, "Unknown API key issue"


def print_analysis_results(response):
    """Print the analysis results in a formatted way"""
    print("\n" + "=" * 80)
    print("AI ANALYSIS RESULTS")
    print("=" * 80)

    if isinstance(response, dict) and "error" in response:
        print(f"❌ Error: {response['error']}")
    elif isinstance(response, str):
        # Count the number of items found
        lines = [line.strip() for line in response.split('\n') if line.strip() and ':' in line]
        print(f"✓ Successfully processed {len(lines)} funeral service items")
        print("\nFormatted Price List:")
        print("-" * 40)
        print(response)
    else:
        print("❌ Unexpected response format")
        print(response)


# Backwards compatibility
def call_gemini_api_legacy(filename, api_key):
    """Legacy function name for backwards compatibility"""
    return call_gemini_api(filename, api_key)


# Add a main function to test the API
if __name__ == "__main__":
    import os
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("❌ GEMINI_API_KEY environment variable not set!")
        print("Please set your API key in the .env file")
        exit(1)
    
    print("Testing Gemini API connection...")
    is_valid, message = test_api_key(api_key)
    
    if is_valid:
        print(f"✓ {message}")
    else:
        print(f"❌ {message}")
        print("\nTo fix this:")
        print("1. Get a new API key from https://makersuite.google.com/app/apikey")
        print("2. Make sure Gemini API is enabled in Google Cloud Console")
        print("3. Check your API quota and billing settings")
        print("4. Set the GEMINI_API_KEY environment variable")