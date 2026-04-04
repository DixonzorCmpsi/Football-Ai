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
        driver = uc.Chrome(options=options, driver_executable_path="/usr/local/bin/chromedriver", use_subprocess=False)
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

def click_prop_category_tabs(driver):
    """
    Click on specific prop category tabs to expand their content.
    These are the exact tab names that appear in Bovada's UI.
    """
    # Priority-ordered list of prop categories we want to scrape
    PROP_CATEGORIES = [
        "Passing Yards",
        "Rushing Yards", 
        "Receiving Yards",
        "Receiving Props",
        "Passing Props",
        "TD Scorer Props",
        "Touchdown Props",
        "Combined Yards",
        "Alternate Lines",
        "Game Props"
    ]
    
    collected_lines = []
    clicked_tabs = set()
    
    for category in PROP_CATEGORIES:
        try:
            # Use JavaScript to find and click elements with exact text match
            clicked = driver.execute_script("""
                const targetText = arguments[0];
                
                // Find all elements that might be tabs
                const allElements = document.querySelectorAll('*');
                
                for (let el of allElements) {
                    // Check if this element's direct text matches our target
                    const directText = Array.from(el.childNodes)
                        .filter(n => n.nodeType === Node.TEXT_NODE)
                        .map(n => n.textContent.trim())
                        .join('');
                    
                    const fullText = el.textContent.trim();
                    
                    // Match exact text or text that starts with our category
                    if (directText === targetText || fullText === targetText) {
                        // Check if element is visible and likely clickable
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            // Scroll into view and click
                            el.scrollIntoView({behavior: 'smooth', block: 'center'});
                            el.click();
                            return true;
                        }
                    }
                }
                return false;
            """, category)
            
            if clicked:
                print(f"    [+] Clicked category: {category}")
                clicked_tabs.add(category)
                time.sleep(2.5)  # Wait for content to load
                
                # Expand any accordions/show more buttons in the newly loaded section
                expand_visible_accordions(driver)
                time.sleep(1)
                
                # Scrape the current page content
                text = scrape_page_text(driver)
                lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 0]
                collected_lines.extend(lines)
                
        except Exception as e:
            print(f"    [!] Error clicking {category}: {e}")
    
    print(f"    [✓] Clicked {len(clicked_tabs)} prop categories")
    return collected_lines

def click_show_more_buttons(driver):
    """Click all 'Show More' or '+' buttons to expand prop lists."""
    try:
        driver.execute_script("""
            // Find and click Show More buttons
            const buttons = document.querySelectorAll('button, [role="button"], .show-more, [class*="show-more"]');
            for (let btn of buttons) {
                const text = btn.textContent.toLowerCase();
                if (text.includes('show more') || text.includes('show all') || text === '+' || text.includes('more props')) {
                    try {
                        btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                        btn.click();
                    } catch(e) {}
                }
            }
            
            // Also try clicking any collapsed accordions
            const accordions = document.querySelectorAll('[class*="accordion"]:not([class*="expanded"]), [class*="collapse"]:not([class*="show"])');
            for (let acc of accordions) {
                try {
                    acc.click();
                } catch(e) {}
            }
        """)
        time.sleep(1.5)
    except Exception as e:
        print(f"    [!] Error expanding buttons: {e}")

def scrape_alternate_props_section(driver):
    """
    Specifically target and expand alternate prop sections which contain 
    the Over/Under lines we need (Rushing Yards, Receiving Yards, etc.)
    """
    alternate_sections = []
    
    try:
        # Find all "Alternate" headers and click to expand them
        result = driver.execute_script("""
            const sections = [];
            const allElements = document.querySelectorAll('*');
            
            for (let el of allElements) {
                const text = el.textContent || '';
                // Look for Alternate prop headers
                if (text.startsWith('Alternate ') && text.includes(' - ')) {
                    // This is likely a prop header like "Alternate Rushing Yards - Player Name"
                    sections.push(text.substring(0, 100));  // Truncate for logging
                    
                    // Try to find and click any expand button near this element
                    const parent = el.closest('[class*="market"], [class*="prop"], [class*="bet"]');
                    if (parent) {
                        const expandBtn = parent.querySelector('[class*="expand"], [class*="toggle"], button');
                        if (expandBtn) {
                            try { expandBtn.click(); } catch(e) {}
                        }
                    }
                }
            }
            return sections.length;
        """)
        
        if result > 0:
            print(f"    [+] Found {result} alternate prop sections")
            time.sleep(1)
            
    except Exception as e:
        print(f"    [!] Error in alternate props: {e}")
    
    return alternate_sections

def click_and_scrape_tabs(driver, current_text):
    """
    Enhanced tab clicking that specifically targets prop categories.
    """
    collected_text = current_text
    
    # Step 1: Click on main prop category tabs
    category_lines = click_prop_category_tabs(driver)
    if category_lines:
        collected_text += "\n" + "\n".join(category_lines)
    
    # Step 2: Expand all "Show More" buttons
    click_show_more_buttons(driver)
    
    # Step 3: Target alternate prop sections
    scrape_alternate_props_section(driver)
    
    # Step 4: Final page scrape after all expansions
    time.sleep(1)
    expand_visible_accordions(driver)
    final_text = scrape_page_text(driver)
    collected_text += "\n" + final_text
    
    # Legacy keyword-based tab clicking for any missed tabs
    keywords = ["Quarterback Props", "Passing Props", "Rushing Props", "Receiving Props"]
    
    for key in keywords:
        try:
            xpath = f"//*[contains(text(), '{key}')]"
            elements = driver.find_elements(By.XPATH, xpath)
            
            for el in elements:
                try:
                    tag = el.tag_name.lower()
                    if tag in ['button', 'a', 'li', 'span', 'div'] and el.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", el)
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", el)
                        print(f"    [+] Clicked legacy tab: {key}")
                        time.sleep(2)
                        expand_visible_accordions(driver)
                        new_text = scrape_page_text(driver)
                        collected_text += "\n" + new_text
                        break
                except: continue
        except Exception as e:
            pass
            
    return collected_text

def scrape_game(driver, url):
    print(f"\n🚀 Processing: {url}")
    driver.get(url)
    time.sleep(5)
    
    # 1. Create Game Folder
    game_id = extract_game_id_from_url(url)
    game_folder = os.path.join(DATA_DIR, game_id)
    if not os.path.exists(game_folder): os.makedirs(game_folder)

    # 2. Scroll down the page to trigger lazy loading
    try:
        driver.execute_script("""
            // Scroll down in increments to trigger lazy loading
            const scrollHeight = document.body.scrollHeight;
            let currentPosition = 0;
            const scrollStep = 500;
            
            while (currentPosition < scrollHeight) {
                window.scrollTo(0, currentPosition);
                currentPosition += scrollStep;
            }
            // Scroll back to top
            window.scrollTo(0, 0);
        """)
        time.sleep(2)
    except: pass

    # 3. Expand default view
    expand_visible_accordions(driver)
    
    # 4. Scrape Default Text
    raw_text = scrape_page_text(driver)
    
    # 5. Click Tabs and Scrape More (enhanced version)
    full_text = click_and_scrape_tabs(driver, raw_text)
    
    # 6. Do a final scroll and expansion pass
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        expand_visible_accordions(driver)
        click_show_more_buttons(driver)
        final_text = scrape_page_text(driver)
        full_text += "\n" + final_text
    except: pass
    
    # 7. Deduplicate lines while preserving order
    seen = set()
    lines = []
    for line in full_text.split('\n'):
        line = line.strip()
        if len(line) > 0 and line not in seen:
            seen.add(line)
            lines.append(line)

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