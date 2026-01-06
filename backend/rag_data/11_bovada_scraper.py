import time
import json
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc


# --- CONFIG ---
DATA_DIR = "bovada_data"
# Allow tests to override the input list via env var BOVADA_INPUT_LIST
INPUT_LIST = os.path.join(DATA_DIR, os.getenv('BOVADA_INPUT_LIST', "games_list.json"))

def setup_driver():
    options = uc.ChromeOptions()
    
    # 1. Universal Settings (Both Environments)
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-breakpad")
    options.add_argument("--disable-client-side-phishing-detection")

    # 2. Docker-Specific Detection & Optimization
    # We check for the '.dockerenv' file which exists in almost all containers
    if os.path.exists('/.dockerenv'):
        options.add_argument("--no-sandbox")            # Essential for Docker root
        options.add_argument("--disable-dev-shm-usage") # Uses /tmp to avoid OOM crashes
        options.add_argument("--single-process")
        options.add_argument("--no-zygote")
        print("🐳 Docker detected: Applying sandbox and memory optimizations.")
    else:
        print("💻 Local environment detected: Running standard profile. To reduce memory, set BOVADA_CHUNK_SIZE smaller or enable swap on the host.")

    # 3. Initialize with error handling
    try:
        driver = uc.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"❌ Driver initialization failed: {e}")
        raise

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

def click_and_scrape_tabs(driver, current_text):
    # Keywords for tabs we want to visit
    keywords = ["Touchdown", "Quarterback", "Passing", "Rushing", "Receiving"]
    
    collected_text = current_text
    
    for key in keywords:
        try:
            # Find elements containing the keyword
            xpath = f"//*[contains(text(), '{key}')]"
            elements = driver.find_elements(By.XPATH, xpath)
            
            # Sort elements to prioritize actual tabs/buttons over random text divs
            # This prevents clicking "Rushing Touchdowns" text instead of the "Rushing" tab
            def get_element_priority(el):
                try:
                    tag = el.tag_name.lower()
                    if 'tab' in tag or 'button' in tag: return 0  # Highest priority
                    if tag == 'a': return 1
                    if tag == 'li': return 2
                    return 3  # Lowest priority (div, span, etc)
                except: return 4

            elements.sort(key=get_element_priority)
            
            clicked = False
            for el in elements:
                # Filter for likely clickable items (buttons, tabs, list items)
                try:
                    tag = el.tag_name.lower()
                    # Check if it's a button or inside a clickable container
                    if tag in ['button', 'sp-tab-button', 'a', 'li', 'span', 'div']:
                        if el.is_displayed():
                            # Scroll and click
                            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", el)
                            time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", el)
                            
                            print(f"    [+] Clicked tab: {key} (tag: {tag})")
                            time.sleep(2) # Wait for content load
                            
                            expand_visible_accordions(driver)
                            new_text = scrape_page_text(driver)
                            collected_text += "\n" + new_text
                            clicked = True
                            break
                except: continue
            
        except Exception as e:
            print(f"    [!] Error processing tab {key}: {e}")
            
    return collected_text

def scrape_game(driver, url):
    print(f"\n🚀 Processing: {url}")
    driver.get(url)
    time.sleep(5)
    
    # 1. Create Game Folder
    game_id = extract_game_id_from_url(url)
    game_folder = os.path.join(DATA_DIR, game_id)
    if not os.path.exists(game_folder): os.makedirs(game_folder)

    # 2. Expand default view
    expand_visible_accordions(driver)
    
    # 3. Scrape Default Text
    raw_text = scrape_page_text(driver)
    
    # 4. Click Tabs and Scrape More
    full_text = click_and_scrape_tabs(driver, raw_text)
    
    lines = [l.strip() for l in full_text.split('\n') if len(l.strip()) > 0]

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

    # Batch processing to limit Chromium memory growth and allow periodic restarts
    chunk_size = int(os.getenv('BOVADA_CHUNK_SIZE', '10'))
    delay_seconds = float(os.getenv('BOVADA_DELAY_SECONDS', '2'))

    print(f"Starting scraping in batches of {chunk_size} with {delay_seconds}s delay")

    for start in range(0, len(urls), chunk_size):
        batch = urls[start:start + chunk_size]
        print(f"\n--- Processing batch {start//chunk_size + 1} (size {len(batch)}) ---")

        driver = setup_driver()
        try:
            for url in batch:
                try:
                    scrape_game(driver, url)
                    time.sleep(delay_seconds)
                except Exception as e:
                    print(f"  ⚠️ Error scraping {url}: {e} -- restarting driver for next batch item")
                    try:
                        driver.quit()
                    except: pass
                    time.sleep(1)
                    driver = setup_driver()
        finally:
            try:
                driver.quit()
            except: pass

    print("\n✅ Finished scraping all batches")

if __name__ == "__main__":
    main()