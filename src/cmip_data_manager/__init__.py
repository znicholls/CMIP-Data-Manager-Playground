"""
Playground exploration of CMIP data handling
"""

import importlib.metadata

from cmip_data_manager.config import Settings
from cmip_data_manager.factory import (
    build_client,
    build_file_search_clients,
    open_repository,
)

__version__ = importlib.metadata.version("cmip_data_manager")

__all__ = [
    "Settings",
    "__version__",
    "build_client",
    "build_file_search_clients",
    "open_repository",
]
