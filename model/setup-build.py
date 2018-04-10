import os
from distutils.core import setup
from Cython.Build import cythonize

abspath = os.path.abspath(os.path.dirname(__file__))

setup(
    ext_modules = cythonize(os.path.join(abspath, "scenario.pyx"))
)

