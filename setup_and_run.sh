#!/bin/bash
# Shell script to set up the environment and run the Video Uploader

# Function to display error messages
error_exit() {
    echo "$1" 1>&2
    deactivate 2>/dev/null
    exit 1
}

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    error_exit "Python3 is not installed. Please install Python3 and try again."
fi

# Create a virtual environment named 'venv' if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv || error_exit "Failed to create virtual environment."
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source venv/bin/activate || error_exit "Failed to activate virtual environment."

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip || error_exit "Failed to upgrade pip."

# Install required packages
echo "Installing dependencies..."
pip install -r requirements.txt || error_exit "Failed to install dependencies."

# Run the Video Uploader application
echo "Starting the Video Uploader..."
python3 microfeed.py || error_exit "The application encountered an error."

# Deactivate the virtual environment after the program exits
deactivate
