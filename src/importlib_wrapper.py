"""Load a plugin file by path without the ``imp`` module.

``imp`` was removed in Python 3.12. This is upstream shotgunEvents' fix
(3878d63, "SG-41625 Add support for Python 3.13"), copied on its own so
``master-glitch`` keeps the daemon version our plugins run on.

One change from upstream: the module is put in ``sys.modules`` before it
runs, as ``imp.load_source`` did. Code inside a plugin that looks itself up
there (dataclasses, typing, pickle) keeps working.
"""

import importlib.machinery
import importlib.util
import sys
from types import ModuleType


def load_source(modname: str, filename: str) -> ModuleType:
    """Load and run the Python file at ``filename`` as module ``modname``.

    Args:
        modname: The module name to register the file under.
        filename: Path to the ``.py`` file.

    Returns:
        The loaded module. A second call with the same name runs the file
        again and replaces the entry in ``sys.modules``.
    """
    loader = importlib.machinery.SourceFileLoader(modname, filename)
    spec = importlib.util.spec_from_file_location(modname, filename, loader=loader)
    if spec is None:
        raise ImportError(f"cannot make a module spec for {filename}", name=modname)
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    loader.exec_module(module)
    return module
