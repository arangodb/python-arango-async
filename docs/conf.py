import os
import sys

# Required for autodoc
sys.path.insert(0, os.path.abspath(".."))

project = "python-arango-async"
copyright_notice = "ArangoDB"
author = "Alexandru Petenchea, Anthony Mahanna"
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "sphinx_rtd_theme"
master_doc = "index"

autodoc_member_order = "bysource"
autodoc_typehints = "none"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "aiohttp": ("https://docs.aiohttp.org/en/stable/", None),
    "jwt": ("https://pyjwt.readthedocs.io/en/stable/", None),
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_attr_annotations = True
