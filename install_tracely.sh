#!/bin/bash

# Ensure the script is run with bash
if [ -z "$BASH_VERSION" ]; then
  echo "This script must be run with bash."
  exit 1
fi

# Check if Python 3.11 or higher is installed
PYTHON_VERSION=$(python3 -c 'import sys; print(sys.version_info >= (3, 11))')
if [ "$PYTHON_VERSION" != "True" ]; then
  echo "Python 3.11 or higher is required. Please activate a Python environment with the correct version."
  exit 1
fi

# Ensure a C++ compiler is available (needed to build tracely's infostop/infomap dependency)
if ! command -v g++ >/dev/null 2>&1; then
  echo "Error: A C++ compiler (g++) is required to build tracely's dependencies but was not found."
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

# Install necessary build tools
# setuptools is pinned below 81 because tracely's infostop -> infomap==1.0.6 dependency
# still imports pkg_resources in its setup.py, which setuptools>=81 no longer ships.
echo "Installing build tools..."
python3 -m pip install --upgrade pip
python3 -m pip install "setuptools<81" wheel twine build pybind11

# Build the package
echo "Building the package..."
python3 -m build --wheel --sdist .

# Install the package
# --no-build-isolation is required so infomap's build uses the setuptools<81 pinned
# above instead of pip fetching the latest setuptools into an isolated build env.
echo "Installing the package..."
python3 -m pip install --no-build-isolation dist/tracely-1.0.0-py3-none-any.whl --force-reinstall

echo "Installation complete!"