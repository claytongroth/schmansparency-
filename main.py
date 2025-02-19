from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
from datetime import datetime
from fpds_scraper import process_fpds_urls

def scrape_rows(url):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    data = {
        'Agency': [],
        'Description': [],
        'Uploaded on': [],
        'Link': [],
        'Saved': [],
        'SavedInt': []
    }
    
    try:
        driver.get(url)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
        )
        
        while True:
            try:
                see_more = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, "//tr[td[contains(text(), 'see more')]]"))
                )
                driver.execute_script("arguments[0].scrollIntoView();", see_more)
                see_more.click()
                time.sleep(1)
            except TimeoutException:
                print("No more 'see more' buttons found.")
                break
        
        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        print(f"\nFound {len(rows)} rows")
        
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            
            # Skip "see more" rows
            if len(cells) == 1 and "see more" in cells[0].text.lower():
                continue
            
            if len(cells) >= 5:  # Only process rows with enough cells
                agency = cells[0].text.strip()
                description = cells[1].text.strip()
                upload_date = cells[2].text.strip()
                
                # Extract href from link cell
                link = ''
                try:
                    link_elem = cells[3].find_element(By.TAG_NAME, "a")
                    link = link_elem.get_attribute('href')
                except:
                    link = ''  # Keep empty if no link found
                
                saved = cells[4].text.strip()
                
                # Convert saved amount to integer, handle "SEE FPDS"
                try:
                    saved_int = float(saved.replace('$', '').replace(',', ''))
                except ValueError:
                    saved_int = 0.0
                
                data['Agency'].append(agency)
                data['Description'].append(description)
                data['Uploaded on'].append(upload_date)
                data['Link'].append(link)
                data['Saved'].append(saved)
                data['SavedInt'].append(saved_int)
        
        # Create initial DataFrame
        df = pd.DataFrame(data)
        
        # Take first 10 rows for testing
        test_df = df.head(10).copy()
        print(f"\nTesting with {len(test_df)} rows...")
        
        # Process FPDS data
        print("\nProcessing FPDS data...")
        enhanced_df = process_fpds_urls(test_df)
        
        # Print stats about the processing
        print(f"\nOriginal columns: {list(test_df.columns)}")
        print(f"Enhanced columns: {list(enhanced_df.columns)}")
        print(f"\nNumber of URLs processed: {len(test_df)}")
        print(f"Number of successful FPDS extractions: {sum(1 for url in test_df['Link'] if isinstance(url, str) and 'fpds.gov' in url.lower())}")
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"savings_data_{timestamp}.csv"
        
        # Save to CSV
        enhanced_df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
        
        # Calculate and print total in billions
        total_billions = df['SavedInt'].sum() / 1_000_000_000
        print(f"\nTotal savings: ${total_billions:.2f} billion")
        

        

        # Print detailed view of enhanced DataFrame
        print("\nDetailed view of enhanced DataFrame:")
        pd.set_option('display.max_columns', None)  # Show all columns
        # Print first few rows of DataFrame
        print("\nFirst few rows of DataFrame:")
        print(enhanced_df.head())


        return enhanced_df
                
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    
    finally:
        driver.quit()

def main():
    url = "https://doge.gov/savings"  # Replace with your target website URL
    try:
        df = scrape_rows(url)
    except Exception as e:
        print(f"Failed to create DataFrame: {e}")

if __name__ == "__main__":
    main()