import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import parse_qs, urlparse
import json

def get_fpds_data(url):
    """
    Fetches and parses FPDS data from a URL, handling the redirect.
    """
    if not url or not isinstance(url, str):
        print(f"Skipping invalid URL: {url}")
        return {}
        
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    try:
        response = session.get(url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        data = {}
        
        rows = soup.find_all('tr')
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if label and value:
                    data[label] = value
        
        return data if data else {}
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {str(e)}")
        return {}
    except Exception as e:
        print(f"Error processing URL {url}: {str(e)}")
        return {}

def process_fpds_urls(df, url_column='Link'):
    """
    Process all FPDS URLs in a DataFrame and store results in a single JSON column
    """
    result_df = df.copy()
    fpds_data = []
    
    print(f"Processing {len(df)} FPDS URLs...")
    for idx, row in df.iterrows():
        url = row[url_column]
        print(f"Processing FPDS URL {idx + 1}/{len(df)}: {url}")
        
        # Only process URLs that contain 'fpds.gov'
        if isinstance(url, str) and 'fpds.gov' in url.lower():
            data = get_fpds_data(url)
            # Convert to JSON string
            json_data = json.dumps(data) if data else None
        else:
            print(f"Skipping non-FPDS URL: {url}")
            json_data = None
            
        fpds_data.append(json_data)
        time.sleep(2)  # Rate limiting
    
    # Add the FPDS data as a new column
    result_df['fpds_data'] = fpds_data
    
    return result_df