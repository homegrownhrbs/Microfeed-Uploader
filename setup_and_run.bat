@echo off
REM Batch script to set up the environment and run the Video Uploader

REM Check if Python is installed
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install Python and try again.
    pause
    exit /b 1
)

REM Create a virtual environment named 'venv' if it doesn't exist
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
    IF %ERRORLEVEL% NEQ 0 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
)

REM Activate the virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to activate virtual environment.
    pause
    exit /b 1
)

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to upgrade pip.
    pause
    exit /b 1
)

REM Install required packages
echo Installing dependencies...
pip install -r requirements.txt
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to install dependencies.
    pause
    exit /b 1
)

REM Run the Video Uploader application
echo Starting the Video Uploader...
python microfeed.py
IF %ERRORLEVEL% NEQ 0 (
    echo The application encountered an error.
    pause
    exit /b 1
)

REM Deactivate the virtual environment after the program exits
deactivate
