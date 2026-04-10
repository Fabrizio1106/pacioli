"""
===============================================================================
Project: PACIOLI
Module: utils.text_normalizer
===============================================================================

Description:
    Text canonicalization helper used to prepare strings for fuzzy
    comparisons across master data sources. Applies uppercasing, accent
    stripping and punctuation removal to produce a stable comparable form.

Responsibilities:
    - Remove diacritical marks via Unicode NFD decomposition.
    - Uppercase and strip non-alphanumeric characters (keeping spaces).
    - Collapse repeated whitespace.

Key Components:
    - normalize_text: Return a canonicalized representation of a string.

Notes:
    - Returns an empty string for falsy or non-string inputs, ensuring the
      function is safe to use inside vectorized pandas operations.

Dependencies:
    - re
    - unicodedata

===============================================================================
"""

import re
import unicodedata

def normalize_text(text):
    """
    Clean and standardize a text value for fuzzy comparisons.

    Args:
        text: Any value. Non-string or falsy inputs return ''.

    Returns:
        str: Uppercase, accent-free, punctuation-free representation with
        a single space between tokens.

    Notes:
        Example: 'LA MORERIA S.A.S.' -> 'LA MORERIA SAS'.
    """
    if not text or not isinstance(text, str):
        return ""
    
    # 1. Convert to uppercase and remove accents
    text = "".join(
        c for c in unicodedata.normalize('NFD', text.upper())
        if unicodedata.category(c) != 'Mn'
    )
    
    # 2. Remove periods, commas, and special characters (except spaces)
    text = re.sub(r'[^A-Z0-9\s]', '', text)
    
    # 3. Remove double spaces and leading/leaving spaces
    text = " ".join(text.split())
    
    return text