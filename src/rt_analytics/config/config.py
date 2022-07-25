"""This module provides ``rt_analytics.config`` with the functionality to load one
or more configuration files from specified paths.
"""
import logging
from glob import iglob
from pathlib import Path
from typing import AbstractSet, Any, Dict, Iterable, List, Tuple, Union

import anyconfig

SUPPORTED_EXTENSIONS = [
    ".yml",
    ".yaml",
    ".json",
    ".ini",
    ".pickle",
    ".properties",
    ".xml",
    ".shellvars",
]


class MissingConfigException(Exception):
    """Raised when no configuration files can be found within a config path"""

    pass


class ConfigLoader:
    """
    Recursively scan the directories specified in ``conf_paths`` for 
    configuration files with a ``yaml``, ``yml``, ``json``, ``ini``,
    ``pickle``, ``xml``, ``properties`` or ``shellvars`` extension,
    load them, and return them in the form of a config dictionary.
    When the same top-level key appears in any 2 config files located in
    the same ``conf_path`` (sub)directory, a ``ValueError`` is raised.
    When the same key appears in any 2 config files located in different
    ``conf_path`` directories, the last processed config path takes
    precedence and overrides this key.

    """

    def __init__(self, conf_paths: Union[str, Iterable[str]]):
        """Instantiate a ConfigLoader.
        Args:
            conf_paths: Non-empty path or list of paths to configuration
                directories.
        Raises:
            ValueError: If ``conf_paths`` is empty.
        """
        if not conf_paths:
            raise ValueError(
                "`conf_paths` must contain at least one path to "
                "load configuration files from."
            )
        if isinstance(conf_paths, str):
            conf_paths = [conf_paths]
        self.conf_paths = list(conf_paths)
        self.logger = logging.getLogger(__name__)

    def get(self, *patterns: str) -> Dict[str, Any]:
        """
        Recursively scan for configuration files, load and merge them, and
        return them in the form of a config dictionary.
        
        Raises:
            ValueError: If 2 or more configuration files inside the same
                config path (or its subdirectories) contain the same
                top-level key.
            MissingConfigException: If no configuration files exist within
                a specified config path.
        
        Returns:
            Dict[str, Any]: A Python dictionary with the combined
                configuration from all configuration files. **Note:** any keys
                that start with `_` will be ignored.
        """

        if not patterns:
            raise ValueError(
                "`patterns` must contain at least one glob "
                "pattern to match config filenames against."
            )

        config = {}  # type: Dict[str, Any]
        processed_files = []

        for conf_path in self.conf_paths:
            new_conf, new_processed_files = _load_config(conf_path, list(patterns))
            common_keys = config.keys() & new_conf.keys()
            if common_keys:
                sorted_keys = ", ".join(sorted(common_keys))
                msg = (
                    "Config from path `%s` will override the following "
                    "existing top-level config keys: %s"
                )
                self.logger.info(msg, conf_path, sorted_keys)
            config.update(new_conf)
            processed_files.extend(new_processed_files)
        if not processed_files:
            raise MissingConfigException(
                "No files found in {} matching the glob "
                "pattern(s): {}".format(str(self.conf_paths), str(list(patterns)))
            )
        return config


def _load_config(
    conf_path: str, patterns: List[str]
) -> Tuple[Dict[str, Any], List[Path]]:
    """Recursively load all configuration files, which satisfy
    a given list of glob patterns from a specific path.
    Args:
        conf_path: Path to a kedro configuration directory.
        patterns: List of glob patterns to match the filenames against.
    Raises:
        ValueError: If 2 or more configuration files contain the same key(s).
    Returns:
        Resulting configuration dictionary.
    """

    conf_path = Path(conf_path)
    if not conf_path.is_dir():
        raise ValueError(
            "Given configuration path either does not exist "
            "or is not a valid directory: {0}".format(conf_path)
        )
    config = {}
    keys_by_filepath = {}  # type: Dict[Path, AbstractSet[str]]

    def _check_dups(file1, conf):
        dups = []
        for file2, keys in keys_by_filepath.items():
            common = ", ".join(sorted(conf.keys() & keys))
            if common:
                if len(common) > 100:
                    common = common[:100] + "..."
                dups.append(str(file2) + ": " + common)

        if dups:
            msg = "Duplicate keys found in {0} and:\n- {1}".format(
                file1, "\n- ".join(dups)
            )
            raise ValueError(msg)

    for path in _path_lookup(conf_path, patterns):
        cfg = {k: v for k, v in anyconfig.load(path).items() if not k.startswith("_")}
        _check_dups(path, cfg)
        keys_by_filepath[path] = cfg.keys()
        config.update(cfg)
    return config, list(keys_by_filepath.keys())


def _path_lookup(conf_path: Path, patterns: List[str]) -> List[Path]:
    """Return a sorted list of all configuration files from ``conf_path`` or
    its subdirectories, which satisfy a given list of glob patterns.
    Args:
        conf_path: Path to configuration directory.
        patterns: List of glob patterns to match the filenames against.
    Returns:
        Sorted list of ``Path`` objects representing configuration files.
    """
    result = set()
    conf_path = conf_path.resolve()

    for pattern in patterns:
        # `Path.glob()` ignores the files if pattern ends with "**",
        # therefore iglob is used instead
        for each in iglob(str(conf_path / pattern), recursive=True):
            path = Path(each).resolve()
            if path.is_file() and path.suffix in SUPPORTED_EXTENSIONS:
                result.add(path)
    return sorted(result)
