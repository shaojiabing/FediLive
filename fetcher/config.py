# fetcher/config.py
import yaml
import os
import logging
import logging.config

class Config:
    """
    Configuration loader for Mastodon Scraper.
    Loads settings from a YAML configuration file.
    """
    def __init__(self, config_path='config/config.yaml'):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with open(config_path, 'r', encoding='utf-8') as file:
            self.config = yaml.safe_load(file)
        
        self.api = self.config.get('api', {})
        self.paths = self.config.get('paths', {})
        self.logging = self.config.get('logging', {})
        
        self.setup_logging()
    
    def setup_logging(self):
        """
        Sets up logging based on the configuration.
        """
        log_level = self.logging.get('level', 'INFO')
        log_file = self.logging.get('file', 'logs/app.log')
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_level.upper(), logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def get_paths(self):
        """
        Retrieves path configurations.
        
        Returns:
            dict: Paths configuration.
        """
        return self.paths