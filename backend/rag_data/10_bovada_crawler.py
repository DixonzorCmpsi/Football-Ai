import time
import json
import os
import sys
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _chrome_driver import build_driver

# --- CONFIG ---
BOVADA_NFL_URL = "https://www.bovada.lv/sports/football/nfl"
DATA_DIR = "bovada_data"
OUTPUT_FILE = os.path.join(DATA_DIR, "games_list.json")

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
def setup_driver():
    options = uc.ChromeOptions()
    
    # 1. Universal Settings (Both Environments)
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # 2. Docker-Specific Detection & Optimization
    # We check for the '.dockerenv' file which exists in almost all containers
    if os.path.exists('/.dockerenv'):
        options.add_argument("--no-sandbox")            # Essential for Docker root
        options.add_argument("--disable-dev-shm-usage") # Uses /tmp to avoid OOM crashes
        print("🐳 Docker detected: Applying sandbox and memory optimizations.")
    else:
        print("💻 Local environment detected: Running standard profile.")

    # 3. Initialize with error handling
    try:
        driver = build_driver(uc, options)
        return driver
    except Exception as e:
        print(f"❌ Driver initialization failed: {e}")
        raise

def main():
    print("--- 🕷️ Starting Bovada Game Crawler ---")
    driver = setup_driver()
    game_links = []

    try:
        driver.get(BOVADA_NFL_URL)
        time.sleep(5) # Let dynamic content load

        # Find all game links (They usually contain '/sports/football/nfl/')
        # excluding futures/props/etc.
        elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/sports/football/nfl/')]")
        
        for el in elements:
            href = el.get_attribute("href")
            # Filter for actual game links (usually end in numbers)
            if href and href[-1].isdigit() and "super-bowl" not in href:
                if href not in game_links:
                    game_links.append(href)

        print(f"    [+] Found {len(game_links)} active game URLs.")
        
        # Save to JSON
        with open(OUTPUT_FILE, "w") as f:
            json.dump(game_links, f, indent=2)
            
        print(f"    [✓] Saved list to {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Crawler Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()