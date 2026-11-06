"""
Builds tracely's one compiled extension (`tracely._stop_event_grouping`, a
small Cython module that replaces the stop-detection grouping logic the
unmaintained `infostop` package used to provide). All other packaging
metadata lives declaratively in `pyproject.toml`; this file exists only
because `ext_modules` cannot be expressed there.
"""
from setuptools import setup
from Cython.Build import cythonize
import numpy as np

setup(
    ext_modules=cythonize(
        "src/tracely/stop_detection/_stop_event_grouping.pyx",
        language_level=3,
        compiler_directives={"boundscheck": False, "wraparound": False, "cdivision": True},
    ),
    include_dirs=[np.get_include()],
)
