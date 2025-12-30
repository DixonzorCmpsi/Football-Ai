import time
import json
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# --- CONFIG ---
DATA_DIR = "bovada_data"
INPUT_LIST = os.path.join(DATA_DIR, "games_list.json")

def setup_driver():
    options = Options()
    # options.add_argument("--headless=new") 
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def clean_filename(text):
    return re.sub(r'[^a-zA-Z0-9]', '_', text)

def extract_game_id_from_url(url):
    # Extracts "team-a-team-b-2025..." from URL
    parts = url.split("/")
    return parts[-1]

def safe_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", element)
        return True
    except:
        return False

def expand_visible_accordions(driver):
    try:
        driver.execute_script("""
            let accs = document.querySelectorAll('sp-accordion');
            for (let a of accs) { if (!a.className.includes('expanded')) a.click(); }
            let btns = document.querySelectorAll('button');
            for (let b of btns) {
                if (b.innerText.includes('Show More') || b.innerText.includes('+')) {
                    b.click();
                }
            }
        """)
        time.sleep(1.5)
    except: pass

def scrape_page_text(driver):
    try:
        container = driver.find_element(By.CSS_SELECTOR, ".sp-main-content")
        return container.text
    except:
        return driver.find_element(By.TAG_NAME, "body").text

def scrape_game(driver, url):
    print(f"\n🚀 Processing: {url}")
    driver.get(url)
    time.sleep(5)
    
    # 1. Create Game Folder
    game_id = extract_game_id_from_url(url)
    game_folder = os.path.join(DATA_DIR, game_id)
    if not os.path.exists(game_folder): os.makedirs(game_folder)

    # 2. Find Tabs (We generally want 'Anytime Touchdown' or 'Player Props')
    # For this architecture, we scrape the *current active view* which defaults to main lines + some props
    # To get everything, we might need to click tabs. For now, let's grab the MAIN view which covers 80% of needs.
    
    # Expand everything
    expand_visible_accordions(driver)
    
    # Scrape Text
    raw_text = scrape_page_text(driver)
    lines = [l.strip() for l in raw_text.split('\n') if len(l.strip()) > 0]

    # Save Menu.json
    output_file = os.path.join(game_folder, "Menu.json")
    data = {
        "url": url,
        "scraped_at": datetime.now().isoformat(),
        "raw_lines": lines
    }
    
    with open(output_file, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"    [✓] Saved {len(lines)} lines to {output_file}")

def main():
    if not os.path.exists(INPUT_LIST):
        print(f"❌ No game list found at {INPUT_LIST}. Run crawler first.")
        return

    with open(INPUT_LIST, 'r') as f:
        urls = json.load(f)

    if not urls:
        print("⚠️ No games in list.")
        return

    driver = setup_driver()
    try:
        for url in urls:
            scrape_game(driver, url)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()