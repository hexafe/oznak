"""Session management for oznak applicaiton state and config"""

import json
import os
import yaml
from typing import Optional, Dict, Any, List
import pandas as pd


class SessionManager:
    """Manages application session state, config and data persistence"""

    def __init__(self):
        """Initialize session manager with default config"""
        self.session_file = 'oznak_session.json'
        self.config_dir = 'config'
        self.session = self._load_session()
        self.db_config = self._load_database_config()
        self.passwords = self._load_passwords()

    def _load_session(self) -> Dict[str, Any]:
        """Load session data from JSON file
        
        Returns:
            Dictionary containing session data
        """
        if os.path.exists(self.session_file):
            try:
                with open(self.session_file, 'r') as f:
                    return json.load(f)
            except:
                return self._get_default_session()
        else:
            return self._get_default_session()

    def _get_default_session(self) -> Dict[str, Any]:
        """Get detault session structure

        Returns:
            Dictionary with default session structure
        """
        return {
            'data_sources': {},
            'combined_datasets': {},
            'current_source': None,
            'database_config': {},
            'data_files': {}
        }

    def _load_database_config(self) -> Dict[str, Any]:
        """Load database config from YAML file

        Returns:
            Dictionary containing database configurations
        """
        config_path = os.path.join(self.config_dir, 'database.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}

    def _load_passwords(self) -> Dict[str, str]:
        """Load database passwords from YAML file

        Returns:
            Dictionary containing password ref and value
        """
        passwords_path = os.path.join(self.config_dir, 'passwords.yaml')
        if os.path.exists(passwords_path):
            try:
                with open(passwords_path, 'r') as f:
                    password_data = yaml.safe_load(f) or {}
                    return password_data.get('passwords', {})
            except Exception:
                return {}
        else:
            self._create_password_template()
            return {}

    def _create_password_template(self):
        """Create template password file if it doesn't exist"""
        template_path = os.path.join(self._config_dir, 'passwords_template.yaml')
        if not os.path.exists(template_path):
            template_content = """# Database passwords - keep this file secure
            line_a_password: "your password_for_line_a"
            line_b_password: "your_password_for_line_b"
            line_c_password: "your_password_for_line_c"
            """
            with open(template_path, 'w') as f:
                f.write(template_content)

    def _save_session(self):
        """Save current session state to JSON file"""
        with open(self.session_file, 'w') as f:
            json.dump(self.session, f, indent=2, default=str)

    def get_database_config(self, db_name: str) -> Optional[Dict[str, Any]]:
        """Get config for a specific database

        Args:
            db_name: Name of the database config to get

        Returns:
            Dictionary containing database config or None if not found
        """
        return self.db_config.get('databases', {}).get(db_name)

    def get_all_database_configs(self) -> Dict[str, Any]:
        """Get all database configs

        Returns:
            Dictionary containing all database configs
        """
        return self.db_config.get('databases', {})

    def get_combination_config(self) -> Dict[str, Any]:
        """Get data combination config

        Returns:
            Dictionary containing combination config settings
        """
        default_config = {
            'unique_identifier': 'TraceCode',
            'timestamp_column': 'TimeStamp',
            'merge_strategy': 'latest_wins',
            'production_line_column': 'production_line'
        }

        user_config = self.db_config.get('combination', {})
        default_config.update(user_config)
        return default_config

    def get_analysis_config(self) -> Dict[str, Any]:
        """Get analysis config

        Returns:
            Dictionary containing analysis config settings
        """
        return self.db_config.get('analysis', {})

    def get_filters_config(self) -> Dict[str, Any]:
        """Get filters config

        Returns:
            Dictionary containing filters config settings
        """
        return self.db_config.get('filters', {})

    def get_quality_config(self) -> Dict[str, Any]:
        """Get quality config

        Returns:
            Dictionary containing quality config settings
        """
        return self.db_config.get('quality', {})

    def get_password(self, password_ref: str) -> Optional[str]:
        """Get passwird reference

        Args:
            password_ref: Reference to the password

        Returns:
            Password string or None if not found
        """
        return self.passwords.get(password_ref)

    def add_combined_dataset(self, name: str, dataset_info: dict):
        """Add combined dataset information to session

        Args:
            name: Name of the combined dataset
            dataset_info: Information about the combined dataset
        """
        if 'combined_datasets' not in self.session:
            self.session['combined_datasets'] = {}
        self.session['combined_datasets'][name] = dataset_info
        self._save_session()

    def get_combined_dataset(self, name: str) -> Optional[Dict[str, Any]]:
        """Get combined dataset information

        Args:
            name: Name of the combined dataset

        Returns:
            Dictionary containing combined dataset information or None if not found
        """
        return self.session.get('combined_datasets', {}).get(name)

    def list_combined_datasets(self) -> List[str]:
        """List all combined datasets

        Returns:
            List of combined dataset names
        """
        return list(self.session.get('combined_datasets', {}).keys())

    def add_data_source(self, name: str, source_info: dict):
        """Add data source information to session

        Args:
            name: Name of the data source
            source_info: Information about the data source
        """
        if 'data_sources' not in self.session:
            self.session['data_sources'] = {}
        self.session['data_sources'][name] = source_info
        if not self.session.get('current_source'):
            self.session['current_source'] = name
        self._save_session()

    def get_current_source(self) -> Optional[Dict[str, Any]]:
        """Get current data source information

        Returns:
            Dictionary containing current data source information or None if not set
        """
        current_name = self.session.get('current_source')
        if current_name:
            return self.session.get('data_sources', {}).get(current_name)
        return None

    def set_current_source(self, name: str) -> bool:
        """Set current data source

        Args:
            name: Name of the data source to set as current

        Returns:
            True if successful, False if data source not found
        """
        if name in self.session.get('data_sources', {}):
            self.session['current_source'] = name
            self._save_session()
            return True
        return False

    def get_data_sources(self) -> Dict[str, Any]:
        """Get all dat asources

        Returns:
            Dictionary containing all data sources
        """
        return self.session.get('data_sources', {})

    def save_combined_dataframe(self, df: pd.DataFrame, name: str = 'combined_data'):
        """Save combined DataFrame to parquet file

        Args:
            df: DataFrame to save
            name: Name for the saved file
        """
        data_dir = 'data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        file_path = os.path.join(data_dir, f"{name}.parquet")
        df.to_parquet(file_path, index=False)

        if 'data_files' not in self.session:
            self.session['data_files'] = {}
        self.session['data_files'][name] = file_path
        self._save_session()

    def load_combined_dataframe(self, name: str = 'combined_data') -> pd.DataFrame:
        """Load combined DataFrame from parquet file

            Args:
                name: Name of the file to load

            Returns:
                Loaded DataFrame

            Raises:
                FileNotFoundError: If the data file is not found
        """
        data_files = self.session.get('data_files', {})
        file_path = data_files.get(name)

        if file_path and os.path.exists(file_path):
            return pd.read_parquet(file_path)
        else:
            raise FileNotFoundError(f"Data file for {name} not found")

    def get_data_file_path(self, name: str) -> str:
        """Get path to saved data file

        Args:
            name: Name of the data file

        Returns:
            Path to the data file
        """
        data_files = self.session.get('data_files', {})
        return data_files.get(name)

