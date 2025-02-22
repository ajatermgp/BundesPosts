import os
import logging
from pathlib import Path
from typing import List
from enum import Enum

class SortMode(Enum):
    RANDOM = "random"
    ASCENDING = "ascending"
    DESCENDING = "descending"

class Config:
    # Essential Settings
    TESTING_MODE = True
    TEST_ACCOUNT_LIMIT = 800
    ERROR_COOLDOWN = 10  # Seconds between retries
    
    # Credentials
    # INSTAGRAM_EMAIL = os.getenv("INSTAGRAM_USER", "tommymeier711@gmail.com")
    # INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASS", "Individuate24$")
    # INSTAGRAM_EMAIL = os.getenv("INSTAGRAM_USER", "huberh139@gmail.com")
    # INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASS", "InstaScraping2025$")
    INSTAGRAM_EMAIL = os.getenv("INSTAGRAM_USER", "bertha_bearig")
    INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASS", "QKMÖ_b{!vJÖX")
    
    
    # Paths
    DATA_DIR = Path("data")
    ZEESCHUIMER_VERSION = "v1.11.4"
    ZEESCHUIMER_URL = f"https://github.com/digitalmethodsinitiative/zeeschuimer/releases/download/{ZEESCHUIMER_VERSION}/zeeschuimer-{ZEESCHUIMER_VERSION}.xpi"

    # Add these new settings
    MAX_SCROLL_ATTEMPTS = 3
    SCROLL_VERIFICATION_INTERVAL = 30  # Seconds between checks
    MIN_EXPORT_SIZE = 1024  # 1KB
    RATE_LIMIT_EVERY = 5  # Accounts
    RATE_LIMIT_DELAY = 15  # Seconds

    # Testing Configuration
    MIN_FOLLOWERS = 1
    SORT_BY = 'Followers'
    SORT_MODE = SortMode.ASCENDING

    # SORT_MODE = SortMode.ASCENDING   # Low to high followers
    # SORT_MODE = SortMode.DESCENDING  # High to low followers
    # SORT_MODE = SortMode.RANDOM      # Random order
    # SORT_MODE = SortMode.NONE        # No sorting

    # Add to essential settings
    MAX_RETRIES = 3  # Max retries for incomplete collections
    
    TRACKING_COLUMNS = [
        'Account-Link', 'scrape_status', 'last_scraped',
        'Export_Path', 'ZS_count', 'IG_count',
        'IG_Followers', 'IG_Followed',
        'Scrape_Start', 'Scrape_End', 'Notes'
    ]
    
    CSV_COLUMNS = TRACKING_COLUMNS  # Alias for consistency

    def __init__(self):
        # Initialize logger
        self.logger = logging.getLogger('bundesposts.config')
        
        # Initialize paths and environment variables
        self.username = os.getenv("INSTAGRAM_USERNAME")
        self.password = os.getenv("INSTAGRAM_PASSWORD")
        self.zeeschuimer_xpi = Path(os.getenv("ZEESCHUIMER_XPI_PATH", "src/extension/zeeschuimer.xpi")).resolve()
        self.data_dir = Path(os.getenv("DATA_DIR", "data")).resolve()
        self.headless = os.getenv("HEADLESS", "False").lower() == "true"

        # Ensure extensions directory exists
        self.extension_dir = Path("src/extension")
        self.extension_dir.mkdir(exist_ok=True)
        self.zeeschuimer_xpi = self.extension_dir / f"zeeschuimer-{self.ZEESCHUIMER_VERSION}.xpi"
        
        # Try to download Zeeschuimer if not present
        if not self.zeeschuimer_xpi.exists():
            self._download_zeeschuimer()

        self.ACCOUNTS_CSV = self.DATA_DIR / "accounts.csv"
        self.RAW_ACCOUNTS_CSV = self.DATA_DIR / "raw_accounts.csv"
        self.EXPORTS_DIR = self.DATA_DIR / "exports"
        
        # Create required directories
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.EXPORTS_DIR.mkdir(exist_ok=True)
        
        # Scraping parameters
        self.HEADLESS = False
        self.TEST_MODE = True

    @property
    def exports_dir(self) -> Path:
        path = self.DATA_DIR / "exports" 
        path.mkdir(exist_ok=True)
        return path

    @property
    def account_limit(self) -> int:
        return self.TEST_ACCOUNT_LIMIT if self.TESTING_MODE else 2
    
    @property
    def test_accounts(self) -> List[str]:
        if self.TESTING_MODE:
            return ['valentin_christian_abel', 'knut_abraham_mdb', 'sanae_ccaa']
        return []

    def _download_zeeschuimer(self):
        """Download Zeeschuimer XPI if not present"""
        try:
            import requests
            response = requests.get(self.ZEESCHUIMER_URL)
            if response.status_code == 200:
                self.zeeschuimer_xpi.write_bytes(response.content)
                self.logger.info(f"Downloaded Zeeschuimer {self.ZEESCHUIMER_VERSION}")
            else:
                self.logger.warning("Failed to download Zeeschuimer, will use local backup")
        except Exception as e:
            self.logger.warning(f"Error downloading Zeeschuimer: {e}, will use local backup")

    def _preprocess_accounts(self, accounts_df):
        """Helper to preprocess accounts based on config"""
        if self.SORT_BY in accounts_df.columns:
            if self.SORT_MODE == SortMode.ASCENDING:
                return accounts_df.sort_values(self.SORT_BY, ascending=True)
            elif self.SORT_MODE == SortMode.DESCENDING:
                return accounts_df.sort_values(self.SORT_BY, ascending=False)
            elif self.SORT_MODE == SortMode.RANDOM:
                return accounts_df.sample(frac=1)
            else:  # NONE
                return accounts_df
        return accounts_df

config = Config()
