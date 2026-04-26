"""
setup.py — Tamil EDA Platform

This file exists for compatibility with tools that still call
`python setup.py install` or `pip install -e .` on older pip versions.

All real project metadata and dependencies live in pyproject.toml.
This file just calls setuptools.setup() with no arguments — it reads
everything from pyproject.toml automatically.

How to install:
  Development mode (editable, all extras):
      pip install -e ".[all]"

  Production (core only):
      pip install .

  Specific extras:
      pip install -e ".[dev]"          # testing + linting tools
      pip install -e ".[cloud]"        # AWS, GCP, Azure connectors
      pip install -e ".[nlp]"          # torch + sentence-transformers + IndicNLP
      pip install -e ".[ingestion]"    # PDF, image, OCR support
      pip install -e ".[security]"     # Vault, Presidio, rate limiter
      pip install -e ".[translation]"  # DeepL + Google Translate backends
"""

from setuptools import setup

# All configuration is in pyproject.toml.
# This call reads it automatically via PEP 517/518.
setup()
