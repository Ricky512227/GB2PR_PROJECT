import os

# Make the src/gb2pr path available for the package loader
pkg_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'src', 'gb2pr'))
if pkg_path not in __path__:
    __path__.insert(0, pkg_path)

# Import a couple of common utilities from the real package and re-export them
import importlib
try:
    _mod_log = importlib.import_module('gb2pr.logMonitor')
    _mod_common = importlib.import_module('gb2pr.CommonUtlity')
    logger = getattr(_mod_log, 'logger')
    executeCmd = getattr(_mod_common, 'executeCmd')
    __all__ = ['logger', 'executeCmd']
except Exception:
    # If submodules aren't available at import time, expose nothing — tests/imports may import submodules explicitly
    __all__ = []
