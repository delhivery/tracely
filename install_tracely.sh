#!/bin/bash

# Usage:
#   ./install_tracely.sh              Normal install: build a wheel, pip install it into
#                                      the current environment. Use this if you just want
#                                      to use tracely normally in this environment.
#
#   ./install_tracely.sh --build-only Build tracely's one compiled extension in place,
#                                      inside this checkout, and do nothing else -- no
#                                      pip install of tracely itself. Use this if you're
#                                      consuming tracely as a vendored/git-submodule source
#                                      checkout (importing it directly rather than
#                                      installing it), especially if multiple different
#                                      tracely checkouts/versions need to coexist in the
#                                      same environment: a normal install registers
#                                      tracely as a real package for that *whole*
#                                      environment, which would make every other
#                                      submodule-style checkout in the same environment
#                                      resolve to whichever one was installed last, not
#                                      its own files. --build-only never registers
#                                      anything, so it's safe to run independently for as
#                                      many separate checkouts as you need.
#
# Either mode needs a C/C++ compiler present (checked below) to build the one extension;
# in --build-only mode it needs to be re-run any time this checkout is re-cloned fresh or
# updated to a commit that changed the extension's source.

# Ensure the script is run with bash
if [ -z "$BASH_VERSION" ]; then
  echo "This script must be run with bash."
  exit 1
fi

# Abort immediately on any failing command -- without this, a failed build
# step (e.g. a missing build dependency) falls through silently and this
# script prints "Done" at the end regardless of whether anything actually
# built.
set -e

BUILD_ONLY=false
if [ "$1" = "--build-only" ]; then
  BUILD_ONLY=true
elif [ -n "$1" ]; then
  echo "Unknown argument: $1"
  echo "Usage: ./install_tracely.sh [--build-only]"
  exit 1
fi

# Check if Python 3.11 or higher is installed
PYTHON_VERSION=$(python3 -c 'import sys; print(sys.version_info >= (3, 11))')
if [ "$PYTHON_VERSION" != "True" ]; then
  echo "Python 3.11 or higher is required. Please activate a Python environment with the correct version."
  exit 1
fi

# Ensure a C/C++ compiler is available (needed to build tracely's own small compiled
# extension, tracely.stop_detection._stop_event_grouping -- infomap itself installs
# from a prebuilt wheel, no compiler needed for that part)
if ! command -v g++ >/dev/null 2>&1; then
  echo "Error: A C++ compiler (g++) is required to build tracely's compiled extension but was not found."
  echo ""
  echo "Install it for your OS, then re-run this script:"
  echo ""
  echo "  Linux (Debian/Ubuntu):"
  echo "    sudo apt-get update && sudo apt-get install -y build-essential cmake g++ gcc"
  echo ""
  echo "  macOS:"
  echo "    xcode-select --install"
  echo ""
  echo "  Windows (in a Developer Command Prompt / PowerShell, using winget):"
  echo "    winget install --id Kitware.CMake -e"
  echo "    winget install --id Microsoft.VisualStudio.2022.BuildTools -e --override \"--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended\""
  echo ""
  exit 1
fi

if [ "$BUILD_ONLY" = true ]; then
  # Build-only mode: compile the extension in place, touch nothing else -- no pip
  # install of tracely itself, so this environment's "tracely" package name (if any)
  # is left completely alone.
  echo "Installing build tools..."
  python3 -m pip install "setuptools>=61.0" "Cython>=3.1.3" "numpy>=2.3.2"

  echo "Building tracely's compiled extension in place..."
  python3 setup.py build_ext --inplace

  echo "Done. tracely's compiled extension is built in place in this checkout."
else
  # Normal mode: build a real wheel and pip install it into the current environment.
  echo "Installing build tools..."
  python3 -m pip install --upgrade pip
  python3 -m pip install "setuptools>=61.0" wheel twine build Cython

  echo "Building the package..."
  python3 -m build --wheel --sdist .

  # --no-build-isolation ensures the wheel install re-uses the numpy already installed
  # in this environment (matching what tracely.stop_detection._stop_event_grouping was
  # compiled against), instead of pip resolving a separate numpy into an isolated build env.
  echo "Installing the package..."
  TRACELY_WHEEL=$(ls dist/tracely-*.whl)
  python3 -m pip install --no-build-isolation "$TRACELY_WHEEL" --force-reinstall

  echo "Installation complete!"
fi
