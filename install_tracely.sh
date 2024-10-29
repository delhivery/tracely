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

# Install necessary build tools
echo "Installing build tools..."
python3 -m pip install --upgrade pip setuptools wheel twine build

# Build the package
echo "Building the package..."
python3 -m build --wheel --sdist .

# Install the package
echo "Installing the package..."
python3 -m pip install dist/tracely-1.0.0-py3-none-any.whl --force-reinstall

echo "Installation complete!"