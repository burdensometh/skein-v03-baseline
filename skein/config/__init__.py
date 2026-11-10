"""JSON configuration (version 2, version 1 via compatibility mapping): validation and typed settings."""

from skein.config.compat import ConfigDeprecationWarning
from skein.config.loader import SUPPORTED_VERSIONS, load_config, parse_config
from skein.config.model import HttpSettings, ProjectConfig, SpiderConfig
from skein.config.schema import ConfigError, Issue
from skein.config.specs import CONFIG_VERSION

__all__ = [
    "CONFIG_VERSION",
    "SUPPORTED_VERSIONS",
    "ConfigDeprecationWarning",
    "ConfigError",
    "HttpSettings",
    "Issue",
    "ProjectConfig",
    "SpiderConfig",
    "load_config",
    "parse_config",
]
