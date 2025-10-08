# scrapper.py (FINAL PRODUCTION VERSION)
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
from io import StringIO

# Imports for Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

print("--- Starting College Stats Scraper ---")

# --- Setup Selenium Webdriver ---
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("log-level=3")
service = ChromeService(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
print("Selenium WebDriver initialized.")

# --- Load Player Data ---
try:
    players_df = pd.read_csv('players_static.csv')
    players_df = players_df.dropna(subset=['pfr_id'])
    players_df = players_df.head(5)
except FileNotFoundError:
    print("Error: players_static.csv not found.")
    driver.quit()
    exit()

all_college_stats = []
success_count = 0
fail_count = 0

# --- Main Scraping Loop ---
for _, player in tqdm(players_df.iterrows(), total=players_df.shape[0], desc="Scraping player pages"):
    player_id = player['player_id']
    pfr_id = player['pfr_id']
    
    try:
        pfr_url = f"https://www.pro-football-reference.com/players/{pfr_id[0]}/{pfr_id}.htm"
        driver.get(pfr_url)

        pro_soup = BeautifulSoup(driver.page_source, 'html.parser')
        college_link = pro_soup.find('a', href=lambda href: href and "sports-reference.com/cfb/players" in href)
        
        if not college_link:
            fail_count += 1
            time.sleep(1)
            continue
            
        college_url = college_link['href']
        driver.get(college_url)
        
        # Use StringIO to fix the FutureWarning
        page_content = StringIO(driver.page_source)
        college_tables = pd.read_html(page_content, attrs = {'id': ['passing', 'rushing']})
        
        stats_df = None
        if college_tables:
            stats_df = pd.concat(college_tables)

        if stats_df is None or stats_df.empty:
            fail_count += 1
            time.sleep(1)
            continue

        if isinstance(stats_df.columns, pd.MultiIndex):
            stats_df.columns = stats_df.columns.droplevel(0)

        stats_df['Year'] = stats_df['Year'].astype(str)
        season_stats = stats_df[stats_df['Year'].str.match(r'^\d{4}$', na=False)].tail(2)
        
        if not season_stats.empty:
            season_stats['player_id'] = player_id
            all_college_stats.append(season_stats)
            success_count += 1
        else:
            fail_count += 1
        
    except Exception:
        fail_count += 1
        pass

    time.sleep(1.5)

# --- Finalize and Save ---
driver.quit()

if all_college_stats:
    final_df = pd.concat(all_college_stats).reset_index(drop=True)
    final_df.columns = final_df.columns.str.lower().str.replace('[^a-z0-9_]+', '', regex=True)
    
    print(f"\nSuccessfully scraped college stats for {success_count} players.")
    print(f"Failed to find stats for {fail_count} players.")
    final_df.to_csv('college_stats.csv', index=False)
    print("Saved college_stats.csv")
else:
    print(f"\nNo college stats were scraped. Success: {success_count}, Fail: {fail_count}")

print("\n--- College Stats Scraper Finished ---")