from .file_usage import file_sizes, get_file_size
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
