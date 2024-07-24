# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

# Path for package
sys.path.insert(0, Path(__file__).parents[2].resolve().as_posix())
# Path for custom extension
sys.path.insert(0, str(Path(__file__).parent.absolute()))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "UCB Prefect Tools"
copyright = "2024, OIT Data Services"
author = "OIT Data Services"
release = "v6.2.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "task_autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]

# Set up autodoc to include all members
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "special-members": True,
    "show-inheritance": True,
}

autosummary_generate = True
add_module_names = False
autodoc_mock_imports = ["rpy2"]
autodoc_member_order = "bysource"
typehints_defaults = 'comma'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
