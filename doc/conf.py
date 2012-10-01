import sys
from os import path

tree = path.dirname(path.dirname(path.abspath(__file__)))
sys.path.insert(0, tree)

import dmedia


# Project info
project = 'Dmedia'
copyright = '2012, Novacut Inc'
version = dmedia.__version__[:5]
release = dmedia.__version__


# General config
needs_sphinx = '1.1'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.coverage',
]
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
exclude_patterns = ['_build']
pygments_style = 'sphinx'


# HTML config
html_theme = 'default'
html_static_path = ['_static']
htmlhelp_basename = 'Dmediadoc'

# Do something useful with this eventually:
intersphinx_mapping = {'http://docs.python.org/': None}
