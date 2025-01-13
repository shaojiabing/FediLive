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
        
        self.mongodb_central = self.config.get('mongodb_central', {})
        self.mongodb_local = self.config.get('mongodb_local', {})
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
    
    def get_central_mongodb_uri(self):
        """
        Constructs the central MongoDB URI from configuration settings.
        
        Returns:
            str: Central MongoDB connection URI.
        """
        username = self.mongodb_central.get('username')
        password = self.mongodb_central.get('password')
        host = self.mongodb_central.get('host', 'localhost')
        port = self.mongodb_central.get('port', 27017)
        return f"mongodb://{username}:{password}@{host}:{port}"
    
    def get_local_mongodb_uri(self):
        """
        Constructs the local MongoDB URI from configuration settings.
        
        Returns:
            str: Local MongoDB connection URI.
        """
        username = self.mongodb_local.get('username')
        password = self.mongodb_local.get('password')
        host = self.mongodb_local.get('host', 'localhost')
        port = self.mongodb_local.get('port', 27017)
        return f"mongodb://{username}:{password}@{host}:{port}"
    
    def get_api_token(self, worker_id):
        """
        Retrieves the API token for a given worker ID.
        
        Args:
            worker_id (int): The ID of the worker.
        
        Returns:
            str: API token.
        
        Raises:
            IndexError: If worker_id is out of range.
        """
        tokens = self.config.get('tokens', [])
        if worker_id < 0 or worker_id >= len(tokens):
            raise IndexError("Invalid worker_id, out of token list range.")
        return tokens[worker_id]
    
    def get_paths(self):
        """
        Retrieves path configurations.
        
        Returns:
            dict: Paths configuration.
        """
        return self.paths
