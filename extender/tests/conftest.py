"""
File:        conftest.py
Author:      Kevin Auberson
Created:     2026-05-02
Description: pytest configuration for the extender test suite. Adds the
             extender/ source directory to sys.path so tests can import
             modules without an installed package.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
