import time
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# --- CONFIG ---
BOVADA_NFL_URL = "https://www.bovada.lv/sports/football/nfl"
DATA_DIR = "bovada_data"
OUTPUT_FILE = os.path.join(DATA_DIR, "games_list.json")

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

def setup_driver():
    options = Options()
    # options.add_argument("--headless=new") # Uncomment for background run
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

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