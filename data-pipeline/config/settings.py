"""
===============================================================================
Project: PACIOLI
Module: config.settings
===============================================================================

Description:
    Central configuration loader for the PACIOLI pipeline. Reads environment
    variables and a YAML settings file to build a unified configuration object
    accessible throughout the application.

Responsibilities:
    - Load environment variables from a .env file.
    - Resolve the project root path (from env var or file-system fallback).
    - Parse config/settings.yaml into the GLOBAL_SETTINGS dictionary.
    - Build absolute path constants for logs, raw, and processed folders.
    - Ensure required output directories exist at startup.

Key Components:
    - GLOBAL_SETTINGS: Parsed YAML dictionary with all pipeline settings.
    - PATHS: Dict mapping logical path names to absolute Path objects.

Notes:
    - Raises FileNotFoundError if settings.yaml is not found at startup.
    - All directories listed in PATHS (except root) are created automatically.

Dependencies:
    - os, yaml, pathlib.Path
    - python-dotenv (dotenv)

===============================================================================
"""
import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# 1. Load environment variables
load_dotenv()
PROJECT_ROOT_STR = os.getenv("PROJECT_ROOT")

if not PROJECT_ROOT_STR:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
else:
    PROJECT_ROOT = Path(PROJECT_ROOT_STR)

# 2. Load global configuration from YAML
SETTINGS_YAML_PATH = PROJECT_ROOT / "config" / "settings.yaml"

try:
    with open(SETTINGS_YAML_PATH, "r", encoding="utf-8") as f:
        GLOBAL_SETTINGS = yaml.safe_load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"CRITICAL: settings file not found at {SETTINGS_YAML_PATH}")

# 3. Build absolute path map
# Note: the 'failed_folder_name' key was intentionally removed from this section
PATHS = {
    "root": PROJECT_ROOT,
    "logs": PROJECT_ROOT / GLOBAL_SETTINGS["logging"]["folder_name"],
    "raw": PROJECT_ROOT / GLOBAL_SETTINGS["paths"]["raw_folder_name"],
    "processed": PROJECT_ROOT / GLOBAL_SETTINGS["paths"]["processed_folder_name"]
}

# Create base directories if they do not exist
for key, path in PATHS.items():
    if key != "root":
        os.makedirs(path, exist_ok=True)