import logging
import random
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException, NoSuchElementException
from src.config.config import Config, SortMode
#from watchdog.observers import Observer
#from watchdog.events import FileSystemEventHandler
import threading
import selenium.webdriver as webdriver
from selenium.webdriver.common.action_chains import ActionChains
import os
import json  # Required for export validation
from dotenv import load_dotenv
from IPython.display import display, clear_output
import re
import numpy as np

# class FileCreationHandler(FileSystemEventHandler):
#     def __init__(self, event, pattern):
#         super().__init__()
#         self.event = event
#         self.pattern = pattern

#     def on_created(self, event):
#         if event.is_directory:
#             return
#         if self.pattern in event.src_path:
#             self.event.set()

class BundesScraper:
    def __init__(self, config):
        """Config validation"""
        self.config = config
        #self._validate_config()
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.driver = None
        self.zeeschuimer_uuid = None
        self._scroll_count = 0
        
        # Initialize directories
        self._setup_directories()
        
        # Initialize tracking columns
        self.tracking_columns = {
            'Account-Link': 'string',
            'scrape_status': 'string',
            'last_scraped': 'datetime64[ns]',
            'Export_Path': 'string',
            'ZS_count': 'int64',
            'IG_count': 'int64',
            'IG_Followers': 'int64',
            'IG_Followed': 'int64',
            'Scrape_Start': 'datetime64[ns]',
            'Scrape_End': 'datetime64[ns]',
            'Notes': 'string'
        }
        
        # Initialize account processing limits
        self._apply_processing_limits()
        
        # Account initialization
        self.accounts = self._prepare_accounts()
        if self.accounts.empty:
            self._process_raw_accounts()
        
        # Configure GeckoDriver
        self._setup_geckodriver()
        
        self.live_view = None  # Will hold our display object
        self._init_live_view()
        
    # def _validate_config(self):
    #     """Ensure critical .env settings exist"""
    #     required = [
    #         'INSTAGRAM_USERNAME', 'INSTAGRAM_PASSWORD',
    #         'DATA_DIR', 'GECKODRIVER_PATH'
    #     ]
    #     missing = [var for var in required if not hasattr(self.config, var)]
    #     if missing:
    #         raise EnvironmentError(f"Missing .env settings: {', '.join(missing)}")
        
    #     # Path existence checks
    #     if not Path(self.config.DATA_DIR).exists():
    #         raise FileNotFoundError(f"Data directory {self.config.DATA_DIR} not found")
        
    def _setup_directories(self):
        """Initialize required directories"""
        self.zeeschuimer_xpi = Path("src/extension/zeeschuimer.xpi")
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.drivers_dir = Path("src/driver")
        self.drivers_dir.mkdir(exist_ok=True)
        
    def _apply_processing_limits(self):
        """Apply config-based processing limits"""
        if self.config.TESTING_MODE:
            self.logger.info(f"TEST MODE ACTIVE - Limiting to {self.config.TEST_ACCOUNT_LIMIT} accounts")
            self.process_limit = self.config.TEST_ACCOUNT_LIMIT
            self.min_followers = 0  # Bypass follower filter in test mode
        else:
            self.process_limit = None  # No limit
            self.min_followers = self.config.MIN_FOLLOWERS

    def _prepare_accounts(self):
        """Load and filter accounts based on config"""
        if self.config.ACCOUNTS_CSV.exists():
            df = pd.read_csv(self.config.ACCOUNTS_CSV, dtype="string")
            
            # Apply follower filter only in production mode
            if not self.config.TESTING_MODE:
                df = df[df['IG_Followers_manual'] >= self.min_followers]
                
            # Apply account limit
            if self.process_limit:
                df = df.head(self.process_limit)
                
            return df
            
        return pd.DataFrame(columns=self.tracking_columns.keys())

    def _process_raw_accounts(self):
        """Process raw accounts with proper column escaping"""
        raw_path = self.data_dir / "raw_accounts.csv"
        
        try:
            # Read with flexible settings
            raw_df = pd.read_csv(
                raw_path,
                sep=",",  # Confirmed comma-separated
                dtype=str,
                engine="python",
                skipinitialspace=True,
                on_bad_lines="warn",
                quotechar='"',  # Handle quoted columns
                encoding='utf-8-sig'  # Handle BOM if present
            )
            
            # Clean column names rigorously
            raw_df.columns = (
                raw_df.columns
                .str.strip()
                .str.replace('\r', ' ')  # Handle carriage returns
                .str.replace('\n', ' ')
                .str.replace(r'\s+', ' ', regex=True)  # Collapse multiple spaces
            )
            
            # Use backticks to escape column names with special characters
            processed = raw_df.assign(
                IG_Followers_manual=lambda df: df['Followers'].apply(self._parse_metric_number)
            ).query(
                "`IG_Followers_manual` > 0 and "
                "`Account-Link`.str.match('^https://www\.instagram\.com/[a-zA-Z0-9_.]+/?$')"
            )
            
            # Create tracking columns
            processed = pd.DataFrame({
                # Core identifier
                'Account-Link': processed['Account-Link'].str.strip(),
                
                # Manual follower data (column 2)
                'IG_Followers_manual': processed['IG_Followers_manual'],
                
                # Tracking columns
                'scrape_status': 'pending',
                'last_scraped': pd.NaT,
                'Export_Path': '',
                'ZS_count': 0,
                'IG_count': 0,
                'IG_Followers': 0,  # Will be populated during scraping
                'IG_Followed': 0,
                'Scrape_Start': pd.NaT,
                'Scrape_End': pd.NaT,
                'Notes': ''  # Empty for manual notes
            })
            
            # Preserve all original columns from raw data
            original_columns = raw_df.drop(columns=['Account-Link', 'Followers'], errors='ignore')
            processed = pd.concat([processed, original_columns], axis=1)
            
            # Apply sorting based on config
            processed = self._apply_sorting(processed)
            
            # Apply test mode limit after validation
            if self.config.TESTING_MODE:
                processed = processed.head(self.config.TEST_ACCOUNT_LIMIT)
            
            self.accounts = processed
            
            # Save filtered accounts
            processed.to_csv(self.config.ACCOUNTS_CSV, index=False)
            self.logger.info(f"Saved {len(processed)} accounts")
            
        except Exception as e:
            self.logger.error("Final processing failure. Columns detected: %s", raw_df.columns.tolist())
            raise

    def _save_accounts(self):
        """Preserve original order while maintaining sorted display"""
        # Keep original unsorted version for saving
        original_df = self.accounts.copy()
        
        # Convert all tracking columns to strings before save
        track_cols = ['ZS_count', 'IG_count', 'IG_Followers', 'IG_Followed']
        original_df[track_cols] = original_df[track_cols].astype(str)
        
        # Save original order CSV
        original_df.to_csv(self.data_dir / "accounts.csv", index=False)
        
        # Maintain sorted display version separately
        if 'IG_Followers_manual' in original_df.columns:
            self.display_df = original_df.sort_values('IG_Followers_manual', ascending=False)
        else:
            self.display_df = original_df.copy()

    def _convert_followers(self, val):
        """Handle follower count conversion"""
        original = str(val).strip()
        if original == '-':
            return 0
        try:
            # Remove thousand separators
            without_thousands = original.replace('.', '')
            # Handle decimal commas
            normalized = without_thousands.replace(',', '.')
            # Extract numeric part
            clean = ''.join([c for c in normalized if c.isdigit() or c in ('.', '-')])
            if not clean:
                return 0
                
            # Process suffixes
            multiplier = 1
            if 'T' in original.upper():
                multiplier = 1000
            elif 'M' in original.upper():
                multiplier = 1_000_000
            elif 'K' in original.upper():
                multiplier = 1000
                
            return int(float(clean) * multiplier)
            
        except Exception as e:
            self.logger.error(f"Follower conversion failed: {original} → {e}")
            return 0

    def _setup_geckodriver(self):
        """Setup and configure geckodriver"""
        self.driver_path = self.drivers_dir / "geckodriver"
        
        if not self.driver_path.exists():
            from webdriver_manager.firefox import GeckoDriverManager
            import shutil
            
            driver_manager = GeckoDriverManager()
            downloaded_path = Path(driver_manager.install())
            
            shutil.move(downloaded_path, self.driver_path)
            os.chmod(self.driver_path, 0o755)
            
            # Clean up cache
            cache_dir = downloaded_path.parent
            try:
                shutil.rmtree(cache_dir)
                self.logger.info(f"Cleaned up geckodriver cache: {cache_dir}")
            except Exception as e:
                self.logger.warning(f"Could not clean up geckodriver cache: {e}")
            
            self.logger.info(f"Moved geckodriver to project directory: {self.driver_path}")

        self.service = Service(executable_path=str(self.driver_path))
        self.options = self._configure_firefox()

    def _human_type(self, element, text):
        """Simulate human typing patterns"""
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.2))
            
    def _human_delay(self, base=5):  # Increased base delay
        """Randomized wait patterns"""
        delay = base * random.uniform(0.7, 2.0)  # More variance
        if random.random() < 0.2:  # 20% chance for extra pause
            delay += random.uniform(3, 7)
        time.sleep(delay)


    def start_browser(self):
        """Complete workflow executor"""
        try:
            # Verify we're using the project's geckodriver
            if not self.driver_path.exists():
                raise RuntimeError(f"Geckodriver not found at {self.driver_path}")
            
            self.logger.info(f"Using geckodriver from: {self.driver_path}")
            
            # Ensure driver is executable
            if not os.access(self.driver_path, os.X_OK):
                os.chmod(self.driver_path, 0o755)    

            # Browser setup
            self.driver = webdriver.Firefox(service=self.service, options=self.options)
            
            # Login sequence
            self.logger.info("Opening browser on random wikipedia page as test")
            self.driver.get("https://en.wikipedia.org/wiki/Special:Random")
            
            self._human_delay(2)
            self.logger.info("Browser opened")
            

        except Exception as e:
            self.logger.critical(f"Fatal error: {str(e)}", exc_info=True)
            raise
        # finally:
        #     if self.driver:
        #         self.driver.quit()
        #     self.logger.info("Scraper shutdown complete")

    # Browser Setup
    def _init_browser(self):
        """Robust driver initialization with WebDriver Manager"""
        from webdriver_manager.firefox import GeckoDriverManager
        
        try:
            # Proper service configuration
            service = Service(GeckoDriverManager().install())
            self.driver = webdriver.Firefox(
                service=service,
                options=self._configure_firefox()
            )
            self.driver.implicitly_wait(10)
            self.logger.info("Firefox initialized successfully")
        except Exception as e:
            self.logger.critical("Browser initialization failed")
            raise RuntimeError("Critical browser failure") from e

    def _configure_firefox(self):
        """Atomic download configuration"""
        options = webdriver.FirefoxOptions()
        
        # Critical path settings (single source of truth)
        exports_path = (self.data_dir / "exports").resolve()  # Make path absolute
        exports_path.mkdir(parents=True, exist_ok=True)  # Explicit directory creation
        
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", str(exports_path))
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/ndjson")
        
        # Security and automation settings
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)
        options.set_preference("pdfjs.disabled", True)
        
        return options
        
    # Zeeschuimer Management
    def _setup_zeeschuimer(self, retries=3):
        """Fixed setup with tab cleanup"""
        for attempt in range(retries):
            try:
                self._close_all_tabs_except(self.driver.current_window_handle)
                # 1. Install extension
                self.logger.info("Installing Zeeschuimer extension")
                self.driver.install_addon(str(self.zeeschuimer_xpi), temporary=True)
                time.sleep(5)  # Critical for extension initialization
                
                # 2. Get UUID via debugging interface
                self._get_zeeschuimer_uuid()
                
                # 3. Enable collection
                self._enable_collection()
                return
            except Exception as e:
                self.logger.warning(f"Zeeschuimer setup failed (attempt {attempt + 1}): {str(e)}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)
            finally:
                self._close_all_tabs_except(self.driver.current_window_handle)  # New cleanup


    def _get_zeeschuimer_uuid(self):
        """Reliable UUID detection from about:debugging"""
        self.driver.get("about:debugging#/runtime/this-firefox")
        time.sleep(3)  # Allow full page load
        
        try:
            extensions = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.card"))
            )
            
            for ext in extensions:
                if "Zeeschuimer" in ext.text:
                    uuid_elem = ext.find_element(By.XPATH, ".//dt[contains(., 'UUID')]/following-sibling::dd")
                    self.zeeschuimer_uuid = uuid_elem.text.strip()
                    self.logger.info(f"Zeeschuimer UUID: {self.zeeschuimer_uuid}")
                    return
                    
            raise RuntimeError("Zeeschuimer extension not found in about:debugging")
        except Exception as e:
            self.logger.error(f"UUID extraction failed: {e}")
            raise

    def _enable_collection(self):
        """Reliable interface initialization with retries"""
        self.driver.get(f"moz-extension://{self.zeeschuimer_uuid}/popup/interface.html")
        self._human_delay(3)
        
        for attempt in range(3):
            try:
                instagram_row = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "stats-instagramcom"))
                )
                toggle = instagram_row.find_element(By.ID, "zs-enabled-instagram.com")
                if not toggle.is_selected():
                    instagram_row.find_element(By.CSS_SELECTOR, "label[for='zs-enabled-instagram.com']").click()
                    self._human_delay(1)
                self.logger.info("Instagram collection enabled")
                return
            except TimeoutException:
                self.driver.refresh()
                self._human_delay(3)
                
        raise RuntimeError("Failed to enable Instagram collection")
        
        
    def _decline_cookies(self):
        """German cookie banner handling"""
        try:
            banner = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@role,'dialog')]"))
            )
            reject_btn = banner.find_element(By.XPATH, ".//button[contains(., 'ablehnen')]")
            self.driver.execute_script("arguments[0].click();", reject_btn)
            self.logger.info("Declined optional cookies")
            self._human_delay(2)
        except TimeoutException:
            self.logger.debug("No cookie banner present")
            
    def _enter_credentials(self):
        """100% reliable credential entry sequence"""
        # 1. Username entry
        username_field = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.NAME, "username"))
        )
        username_field.click()
        self._human_type(username_field, self.config.INSTAGRAM_EMAIL)
        time.sleep(0.5)  # Critical for UI update
        
        # 2. Password entry
        password_field = self.driver.find_element(By.NAME, "password")
        password_field.click()
        self._human_type(password_field, self.config.INSTAGRAM_PASSWORD)
        time.sleep(0.5)
        
        # 3. Submit with guaranteed click
        submit = self.driver.find_element(By.XPATH, "//button[@type='submit']")
        self.driver.execute_script("arguments[0].click();", submit)
        self._human_delay(3)

    def _dismiss_post_login_modals(self):
        """Original working modal dismissal"""
        #self.driver.refresh()
        
        try:
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Informationen speichern')]"))
            ).click()
            self._human_delay(1)
        except TimeoutException:
            self.logger.debug("No post-login modal found")
            pass
            
    def _close_all_tabs_except(self, keep_handle):
        """Robust tab closure with validation"""
        for handle in list(self.driver.window_handles):  # Copy to prevent mutation issues
            if handle != keep_handle:
                try:
                    self.driver.switch_to.window(handle)
                    self.driver.close()
                except WebDriverException:
                    pass
        self.driver.switch_to.window(keep_handle)
        assert len(self.driver.window_handles) == 1, "Tab cleanup failed"  # Critical check


    def reset_zeeschuimer(self):
        """Robust Zeeschuimer reset with verification"""
        try:
            # 1. Navigate to Zeeschuimer interface
            self.driver.get(f"moz-extension://{self.zeeschuimer_uuid}/popup/interface.html")
            time.sleep(3)  # Allow proper load
            
            # 2. Verify interface loaded
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "stats-instagramcom"))
            )
            
            # 3. Reset if needed
            count_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
            )
            current_count = int(count_elem.text.strip().replace('.', '').replace(',', ''))
            
            if current_count > 0:
                reset_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#stats-instagramcom button.reset"))
                )
                reset_btn.click()
                time.sleep(2)
                
                # Verify reset worked
                count_elem = WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
                )
                final_count = int(count_elem.text.strip().replace('.', '').replace(',', ''))
                assert final_count == 0, "Reset failed: count not zero"
                
            return True
            
        except Exception as e:
            self.logger.error(f"Reset failed: {str(e)}")
            return False

    def get_instagram_post_count(self):
        """Extract post count from Instagram profile"""
        try:
            count_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//header//ul/li[1]//span"))
            )
            count_text = count_element.get_attribute("innerText")
            
            # Extract numeric part before text (e.g. "1298 Beiträge")
            numeric_part = count_text.split()[0]
            clean_count = numeric_part.replace('.', '').replace(',', '').strip()
            return int(clean_count)
        except Exception as e:
            self.logger.error(f"Failed to get Instagram post count: {str(e)}")
            return None


    def get_instagram_follower_count(self):
        """Extract post count from Instagram profile"""
        try:
            count_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//header//ul/li[2]//span"))#/html/body/div[1]/div/div/div[2]/div/div/div[1]/div[2]/div/div[1]/section/main/div/header/section[3]/ul/li[2]/div/a/span/span
            )
            count_text = count_element.get_attribute("innerText")


            # Handle metric suffixes (k/M)
            count = count_text.replace('\xa0', ' ').replace(',', '.').strip()
            if ' Mio' in count:
                return int(float(count.replace(' Mio', '')) * 1_000_000)
            if ' Tsd' in count:
                return int(float(count.replace(' Tsd', '')) * 1_000)
            
            return int(count.replace('.', '').replace(',', '').split()[0])


            # # Clean and convert the count
            # clean_count = count_text.replace('.', '').replace(',', '').strip()
            # return int(clean_count)
        except Exception as e:
            self.logger.error(f"Failed to get Instagram follower count: {str(e)}")
            return None


    def get_instagram_followed_count(self):
        """Extract followed count from Instagram profile"""
        try:
            count_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//header//ul/li[3]//span"))
            )
            count_text = count_element.get_attribute("innerText")
            
            # Extract numeric part before text (e.g. "1701 Gefolgt")
            numeric_part = count_text.split()[0]
            clean_count = numeric_part.replace('.', '').replace(',', '').strip()
            return int(clean_count)
        except Exception as e:
            self.logger.error(f"Failed to get Instagram followed count: {str(e)}")
            return None



    def scroll_profile(self, target_count, max_scrolls=30):
        """Scroll profile with progress monitoring"""
        scrolls = 0
        last_height = 0
        no_change_count = 0
        
        while scrolls < max_scrolls:
            # Scroll down
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1.5, 3))
            scrolls += 1
            
            # Check new height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_change_count += 1
                if no_change_count >= 3:  # If height hasn't changed for 3 consecutive scrolls
                    self.logger.info("Reached apparent bottom of page")
                    break
            else:
                no_change_count = 0
                
            last_height = new_height
            self.logger.info(f"Scroll {scrolls}: Height {new_height}")
            
            # Every 5 scrolls, check Zeeschuimer count
            if scrolls % 20 == 0:
                if self.check_zeeschuimer_progress(target_count):
                    self.logger.info("Reached target post count")
                    return True
                    
        return False

    def check_zeeschuimer_progress(self, target_count):
        """Check current Zeeschuimer count against target"""
        try:
            # Store current handle
            current_handle = self.driver.current_window_handle

            # Close any accidental extra tabs
            while len(self.driver.window_handles) > 2:
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.close()


            # Switch to Zeeschuimer tab (first window handle)
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.refresh()
            time.sleep(2)
            
            # Get count
            count_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
            )
            current_count = int(count_elem.text.strip().replace('.', '').replace(',', ''))
            
            # Return to profile tab
            self.driver.switch_to.window(current_handle)
        
            self.logger.info(f"Progress: {current_count}/{target_count} posts collected")
            return current_count >= target_count    
            
        except Exception as e:
            self.logger.error(f"Failed to check Zeeschuimer progress: {str(e)}")
            self.driver.switch_to.window(current_handle)
            return False

    def get_latest_export(self, export_dir, timestamp_threshold):
        """Get the most recently created .ndjson file in export directory"""
        try:
            export_files = list(export_dir.glob("zeeschuimer-export-instagram.com-*.ndjson"))
            if export_files:
                # Sort by creation time and get the newest file
                latest_file = max(export_files, key=lambda x: x.stat().st_ctime)
                # Only return if file was created after our threshold
                if latest_file.stat().st_ctime >= timestamp_threshold:
                    return latest_file
        except Exception as e:
            self.logger.error(f"Error finding export file: {str(e)}")
        return None

    def process_profile(self, idx, profile_url):
        """Process a single profile with strict tab management"""
        profile_tab = None
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        metadata = {
            'ZS_count': 0,
            'IG_count': 0,
            'IG_Followers': None,
            'IG_Followed': None,
            'Scrape_Start': start_time,
            'Scrape_End': None,
            'Notes': ''
        }

        try:
            # 1. Ensure Zeeschuimer tab is ready (tab 1) and clean
            while len(self.driver.window_handles) > 1:
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.get(f"moz-extension://{self.zeeschuimer_uuid}/popup/interface.html")
            time.sleep(2)
            
            # Verify Zeeschuimer is clean before starting
            count_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
            )
            if int(count_elem.text.strip()) != 0:
                reset_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#stats-instagramcom button.reset"))
                )
                reset_btn.click()
                time.sleep(2)
                
            # 2. Open profile in new tab (tab 2)
            self.driver.switch_to.new_window('tab')
            profile_tab = self.driver.current_window_handle
            self.driver.get(profile_url)
            time.sleep(3)
            
            # 3. Extract profile metadata
            metadata['IG_count'] = self.get_instagram_post_count()

            if not metadata['IG_count']:
                self.logger.warning(f"Could not get post count for {profile_url}")
                # Proper cleanup before returning
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return False
                
            self.logger.info(f"Found {metadata['IG_count']} posts")
            
            metadata['IG_Followers'] = self.get_instagram_follower_count()

            if not metadata['IG_Followers']:
                self.logger.warning(f"Could not get follower count for {profile_url}")
                # Proper cleanup before returning
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return False
                
            self.logger.info(f"Found {metadata['IG_Followers']} Followers")

            metadata['IG_Followed'] = self.get_instagram_followed_count()

            if not metadata['IG_Followed']:
                self.logger.warning(f"Could not get followed count for {profile_url}")
                # Proper cleanup before returning
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
                return False
                
            self.logger.info(f"Found {metadata['IG_Followed']} Followed")

            
            # 4. Scroll and monitor progress with retry limit
            retries = 0
            last_count = 0
            while True:
                # Scroll in profile tab
                success = self.scroll_profile(metadata['IG_count'])
                
                # Switch to Zeeschuimer tab to check progress
                self.driver.switch_to.window(self.driver.window_handles[0])
                count_elem = WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
                )
                current_count = int(count_elem.text.strip().replace('.', '').replace(',', ''))
                metadata['ZS_count'] = current_count
                
                time.sleep(1)
                self.logger.info(f"Progress: {current_count}/{metadata['IG_count']} posts collected")
                
                # Exit conditions with progressive thresholds
                if retries >= 5:
                    threshold = 0.75 * metadata['IG_count']
                    if current_count >= threshold:
                        metadata['Notes'] = f"Accepting partial capture ({current_count}/{metadata['IG_count']}) after {retries} retries"
                        self.logger.info(metadata['Notes'])
                        break
                    else:
                        metadata['Notes'] = f"Failed capture after {retries} retries: {current_count}/{metadata['IG_count']}"
                        self.logger.warning(metadata['Notes'])
                        break
                elif current_count >= metadata['IG_count']:
                    break
                
                if current_count == last_count:
                    retries += 1
                    self.logger.warning(f"No new posts found, retry {retries}/5")
                else:
                    retries = 0
                last_count = current_count
                
                # Switch back to profile tab for more scrolling
                self.driver.switch_to.window(profile_tab)
                time.sleep(random.uniform(1.5, 3))
                
            # 5. Export    
            pre_export_time = time.time()
            
            export_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#stats-instagramcom button.download-ndjson"))
            )
            export_btn.click()
            time.sleep(5)
            
            export_dir = self.data_dir / "exports"
            latest_export = self.get_latest_export(export_dir, pre_export_time)
            
            if latest_export:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                IG_handle = profile_url.split('/')[-2]
                new_filename = f"{timestamp}_{IG_handle}.ndjson"
                new_path = export_dir / new_filename
                
                try:
                    latest_export.rename(new_path)
                    # Convert all values to strings explicitly
                    self.accounts.at[idx, 'Export_Path'] = str(new_path)
                    self.accounts.at[idx, 'ZS_count'] = str(metadata['ZS_count'])
                    self.accounts.at[idx, 'IG_count'] = str(metadata['IG_count'])
                    self.accounts.at[idx, 'IG_Followers'] = str(metadata['IG_Followers'])
                    self.accounts.at[idx, 'IG_Followed'] = str(metadata['IG_Followed'])
                    # Keep datetime as string
                    self.accounts.at[idx, 'Scrape_End'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    self.logger.info(f"Successfully renamed export to {new_filename}")
                    
                    # Force dtype consistency before save
                    self._enforce_accounts_dtypes()
                    self._save_accounts()
                    
                except Exception as e:
                    self.logger.error(f"Error updating tracking: {str(e)}")
                    self._save_accounts()  # Emergency save

            # 6. Reset Zeeschuimer and verify
            reset_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#stats-instagramcom button.reset"))
            )
            reset_btn.click()
            time.sleep(2)
            
            count_elem = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#stats-instagramcom td.num-items"))
            )
            final_count = int(count_elem.text.strip())
            if final_count != 0:
                self.logger.warning(f"Reset failed, count is {final_count}")
                reset_btn.click()
                time.sleep(2)
                if int(count_elem.text.strip()) != 0:
                    raise Exception("Failed to reset Zeeschuimer count to 0")
            
            self.logger.info("Successfully reset Zeeschuimer")
            
            # Close profile tab
            self.driver.switch_to.window(profile_tab)
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            return True

        except Exception as e:
            self.logger.error(f"Error processing {profile_url}: {str(e)}")
            try:
                if profile_tab and profile_tab in self.driver.window_handles:
                    self.driver.switch_to.window(profile_tab)
                    self.driver.close()
            except:
                pass
            self.driver.switch_to.window(self.driver.window_handles[0])
            return False

    def update_accounts_csv(self, account_data):
        """Atomic CSV update with full column validation"""
        try:
            current_data = pd.read_csv(self.config.ACCOUNTS_CSV)
            
            # Ensure type consistency
            for col, dtype in self.tracking_columns.items():
                if col in current_data:
                    current_data[col] = current_data[col].astype(dtype)
                else:
                    current_data[col] = pd.Series(dtype=dtype)
                
        except FileNotFoundError:
            current_data = pd.DataFrame(columns=self.tracking_columns.keys()).astype(self.tracking_columns)
        
        new_row = pd.DataFrame([account_data]).astype(self.tracking_columns)
        updated = pd.concat([current_data, new_row], ignore_index=True)
        
        # Atomic write
        temp_path = self.config.ACCOUNTS_CSV.with_suffix('.tmp')
        updated.to_csv(temp_path, index=False)
        temp_path.replace(self.config.ACCOUNTS_CSV)
        
        self.logger.info(f"Updated accounts CSV with {account_data['Username']} data")

    def setup_instagram(self):
        try:
            self.driver.get("https://www.instagram.com")
            self._decline_cookies()
            time.sleep(2)  # Fixed delay for cookie dismissal
            
            self._enter_credentials()
            
            # Verify login success
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//nav[contains(@aria-label,'Navigation')]"))
            )

            time.sleep(1)
            self.driver.get("https://www.instagram.com/accounts/onetap/?next=%2F")
            time.sleep(2)
            self._dismiss_post_login_modals()
            return True
        except Exception as e:
            self.logger.error(f"Login failed: {str(e)}", exc_info=True)
            return False
            return False

    def scrape_accounts(self):
        """Add scrape_status initialization"""
        # Initialize status column if missing
        if 'scrape_status' not in self.accounts.columns:
            self.accounts['scrape_status'] = 'pending'
        
        # Original code remains unchanged below
        max_retries = 3
        retry_count = 0
        for idx, row in self.accounts.iterrows():
            while retry_count < max_retries:
                try:
                    # Skip invalid or completed accounts
                    if 'Account-Link' not in row or not row['Account-Link']:
                        self.logger.info(f"Skipping row {idx}: No valid Account-Link")
                        self.accounts.at[idx, 'scrape_status'] = 'skipped'
                        break

                    # Handle new accounts with no data
                    if pd.isna(row['ZS_count']) or pd.isna(row['IG_count']):
                        ig = 0
                        zs = 0
                    else:
                        ig = int(float(row['IG_count']))  # Handle potential float strings
                        zs = int(float(row['ZS_count']))
                    
                    # Only skip if either:
                    # 1. Status is completed/done OR
                    # 2. We've captured at least 90% of posts
                    if (row['scrape_status'] in ['completed', 'done']) or (zs / ig >= 0.9 if ig > 0 else False):
                        self.logger.info(f"Skipping {row['Account-Link']}: {row['scrape_status']} profile")
                        break
                    
                    
                    # Process account
                    self.logger.info(f"\nProcessing profile: {row['Account-Link']}")
                    success = self.process_profile(idx, row['Account-Link'])
                    

                    ######################         
                    if success:
                        # Get fresh counts as integers
                        zs = int(self.accounts.at[idx, 'ZS_count'])
                        ig = int(self.accounts.at[idx, 'IG_count'])
                        
                        # New status hierarchy
                        if zs >= ig:
                            new_status = 'completed'
                        elif (ig - zs) <= 3:  # 1-3 posts missing
                            new_status = 'done'
                            self.logger.info(f"Marked {row['Account-Link']} as done ({ig-zs} posts missing)")
                        else:
                            new_status = 'incomplete'
                        
                        self.logger.info(f"Successfully processed {row['Account-Link']} ({new_status})")
                        self.accounts.at[idx, 'scrape_status'] = new_status
                        self.accounts.at[idx, 'last_scraped'] = datetime.now().isoformat()
                        break

                    retry_count += 1
                    if retry_count == max_retries:
                        self.logger.error(f"Failed to process {row['Account-Link']} after {max_retries} attempts")
                        self.accounts.at[idx, 'scrape_status'] = 'failed'
                    else:
                        self.logger.warning(f"Retrying {row['Account-Link']} (attempt {retry_count + 1}/{max_retries})")
                    time.sleep(5)
                    
                except Exception as e:
                    self.logger.error(f"Error processing {row['Account-Link']}: {str(e)}")
                    retry_count += 1
                    if retry_count == max_retries:
                        self.accounts.at[idx, 'scrape_status'] = 'failed'
                    time.sleep(5)
                    
                finally:
                    # Save progress after each attempt
                    self.accounts.to_csv(self.data_dir / "accounts.csv", index=False)
                    time.sleep(random.uniform(2, 4))

        print("Scraping finished.")

    def live_progress(scraper):
        """Display sorted version without affecting storage"""
        while scraper.scrape_running:
            clear_output(wait=True)
            print("Live Scraping Progress (Sorted by Followers):")
            display(scraper.display_df)  # Show sorted view
            time.sleep(10)



######## MAYBE GOOD FOR LATER ?! '''''###########################


    def _sanitize_handle(self, profile_url):
        """Sanitize profile URL for export"""
        return profile_url.strip("/").split("/")[-1]

######## mAYBE GOOD FOR FULL SIZE SCRAPES ?! '''''###########################
    # def _reinitialize_driver(self):
    #     """Full driver reset with cleanup"""
    #     try:
    #         # Force kill old driver
    #         if self.driver:
    #             try:
    #                 self.driver.quit()
    #             except Exception as e:
    #                 self.logger.error(f"Driver termination failed: {str(e)}")
            
    #         # Fresh initialization
    #         self.service = Service(GeckoDriverManager().install())
    #         self.options = self._configure_firefox()
    #         self.driver = webdriver.Firefox(
    #             service=self.service, 
    #             options=self.options
    #         )
            
    #         # Stabilization sequence
    #         time.sleep(5)  # Increased stabilization
    #         self._setup_zeeschuimer()
    #         self._login_instagram()
    #         return True
            
    #     except Exception as e:
    #         self.logger.critical(f"Driver reinit failed: {str(e)}")
    #         raise
######## mAYBE GOOD FOR FULL SIZE SCRAPES ?! '''''###########################


############ UNBENUTZT ABER WÄRE EVTL GANZ COOL :            
    # def _persist_account_state(self):
    #     """Save account progress reliably"""
    #     self.accounts.to_csv(self.data_dir / "accounts.csv", index=False)
        
    # def _handle_processing_error(self, error, account_row):
    #     """Centralized error handling"""
    #     self.logger.error(f"Failed processing {account_row['Account-Link']}: {str(error)}")
    #     account_row['scrape_status'] = 'failed'
    #     account_row['last_error'] = str(error)
    #     self._persist_account_state()
        
##############################################################            

    def add_profiles(self, urls):
        """Add new profiles to tracking system"""
        new_profiles = pd.DataFrame({
            'Account-Link': urls,
            'scrape_status': 'pending',
            'last_scraped': pd.NaT,
            'Export_Path': '',
            'ZS_count': 0,
            'IG_count': 0,
            'IG_Followers': 0,
            'IG_Followed': 0,
            'Scrape_Start': pd.NaT,
            'Scrape_End': pd.NaT,
            'Notes': ''
        })
        
        self.accounts = pd.concat([self.accounts, new_profiles], ignore_index=True)
        self._save_accounts()
        self.logger.info(f"Added {len(urls)} new profiles")

    def _init_live_view(self):
        """Create empty progress table with styling"""
        if pd.__version__ >= "1.3.0":
            self.live_view = display(display_id="scraping-progress")
        else:
            self.live_view = display(display_id=True)
            
    def _update_live_view(self):
        """Update the displayed progress table"""
        view_df = self.accounts[[
            'Account-Link', 'scrape_status', 'last_scraped', 
            'IG_Followers', 'IG_count', 'ZS_count'
        ]].copy()
        
        # Apply styling
        styled = (view_df.style
            .bar(subset=['IG_Followers'], color='#5fba7d', vmin=0, vmax=100000)
            .bar(subset=['IG_count'], color='#1f77b4', vmin=0)
            .format({'last_scraped': lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else ''})
            .set_caption("Current Scraping Progress")
        )
        
        # Update display
        clear_output(wait=True)
        self.live_view.update(styled)
    def _get_follower_count(self):
        """Robust follower count parsing with metric suffix support"""
        try:
            element = self.driver.find_element(By.XPATH, "//a[contains(@href,'followers')]/span")
            text = element.get_attribute("title") or element.text
            
            # Handle metric suffixes (k/M) with different language formats
            clean_text = text.replace('\xa0', ' ').replace(',', '.').lower()
            
            if 'mio' in clean_text:  # German "Millionen"
                return int(float(clean_text.replace(' mio', '')) * 1_000_000)
            elif 'tsd' in clean_text:  # German "Tausend"
                return int(float(clean_text.replace(' tsd', '')) * 1_000)
            elif 'm' in clean_text:  # English million
                return int(float(clean_text.replace('m', '')) * 1_000_000)
            elif 'k' in clean_text:  # English thousand
                return int(float(clean_text.replace('k', '')) * 1_000)
                
            return int(float(clean_text.split()[0].replace('.', '')))
        except Exception as e:
            self.logger.warning(f"Could not parse follower count: {str(e)}")
            return 0


    @staticmethod
    def _parse_metric_number(value):
        """Universal metric parser handling all number formats"""
        try:
            # Normalize different decimal separators and suffixes
            clean = str(value).lower() \
                .replace(',', '.') \
                .replace('tsd', 'k') \
                .replace('mio', 'm') \
                .replace('t', 'k') \
                .replace('\xa0', '') \
                .replace(' ', '')
            
            # Extract numeric components and suffix
            match = re.match(r"^([\d\.]+)([mk]?)$", clean)
            if not match:
                return 0
            
            number_str, suffix = match.groups()
            
            # Remove thousand separators and parse
            number = float(number_str.replace('.', '')) if '.' in number_str \
                else float(number_str)
            
            # Apply multipliers
            multipliers = {'k': 1_000, 'm': 1_000_000}
            return int(number * multipliers.get(suffix, 1))
        
        except:
            return 0

    def _apply_sorting(self, df):
        """Safe sorting with index preservation"""
        sorted_df = df.reset_index(drop=True).copy()  # Prevent index corruption
        
        if self.config.SORT_MODE == SortMode.ASCENDING:
            sorted_df = sorted_df.sort_values('IG_Followers_manual', 
                                            ascending=True,
                                            key=lambda x: x.fillna(0))
        elif self.config.SORT_MODE == SortMode.DESCENDING:
            sorted_df = sorted_df.sort_values('IG_Followers_manual', 
                                            ascending=False,
                                            key=lambda x: x.fillna(0))
        else:
            sorted_df = sorted_df.sample(frac=1, random_state=self.config.SEED)
        
        # Maintain original index for CSV mapping
        return sorted_df.reset_index(drop=True)

    def _scroll_profile(self, attempt):
        """Adaptive scrolling with exponential backoff"""
        base_scrolls = 10
        max_scrolls = 30
        scrolls = min(base_scrolls + (attempt * 3), max_scrolls)
        
        for _ in range(scrolls):
            # Simulate human-like scrolling with variable speed and pauses
            scroll_distance = random.randint(1500, 2500)  # Vary scroll distance
            scroll_speed = random.uniform(0.5, 1.5)  # Vary scroll speed
            scroll_script = f"""
                let distance = {scroll_distance};
                let speed = {scroll_speed};
                let start = Date.now();
                function scrollStep() {{
                    let elapsed = Date.now() - start;
                    let progress = Math.min(elapsed / (distance * speed), 1);
                    window.scrollBy(0, distance * progress);
                    if (progress < 1) {{
                        setTimeout(scrollStep, 10);
                    }}
                }}
                scrollStep();
            """
            self.driver.execute_script(scroll_script)
            time.sleep(random.uniform(0.9,1.2) + (attempt * 0.5))  # Progressive delay

    def _scrape_single_account(self, idx):
        """Enhanced scraping with Zeeschuimer buffer"""
        # Initial load buffer
        time.sleep(random.uniform(2.8, 3.2))
        
        # Progressive retries with backoff
        for attempt in range(5):
            try:
                self._scroll_profile(attempt)
                current_count = self._get_zeeschuimer_count()
                
                # Allow partial success if >75% captured
                if current_count >= 0.75 * self.accounts.at[idx, 'IG_count']:
                    break
                
            except NoSuchElementException:
                wait_time = 2 ** (attempt + 1)
                self.logger.warning(f"Retry {attempt+1}/5 - Waiting {wait_time}s")
                time.sleep(wait_time)
        
        # Final buffer for Zeeschuimer
        time.sleep(10)
        self._export_data()

    def _handle_private_account(self):
        """Private account detection and handling"""
        try:
            private = self.driver.find_element(By.XPATH, "//h2[contains(., 'This Account is Private')]")
            if private:
                self.logger.warning("Private account detected - skipping")
                return True
        except NoSuchElementException:
            return False

    def _enforce_accounts_dtypes(self):
        """Atomic type enforcement before save"""
        type_map = {
            'ZS_count': ('int32', lambda x: pd.to_numeric(x, errors='coerce').fillna(0)),
            'IG_count': ('int32', lambda x: pd.to_numeric(x, errors='coerce').fillna(0)),
            'IG_Followers': ('int32', lambda x: pd.to_numeric(x, errors='coerce').fillna(0)),
            'IG_Followed': ('int32', lambda x: pd.to_numeric(x, errors='coerce').fillna(0)),
            'Export_Path': ('string', lambda x: x.astype('string')),
            'Scrape_Start': ('datetime64[ns]', lambda x: pd.to_datetime(x, errors='coerce')),
            'Scrape_End': ('datetime64[ns]', lambda x: pd.to_datetime(x, errors='coerce'))
        }
        
        for col, (dtype, converter) in type_map.items():
            if col in self.accounts.columns:
                try:
                    if dtype.startswith('int'):
                        self.accounts[col] = pd.to_numeric(self.accounts[col], errors='coerce').fillna(0).astype(dtype)
                    elif 'datetime' in dtype:
                        self.accounts[col] = pd.to_datetime(self.accounts[col], errors='coerce')
                    else:
                        self.accounts[col] = self.accounts[col].astype(dtype)
                except Exception as e:
                    self.logger.warning(f"Type enforcement failed for {col}: {str(e)}")

