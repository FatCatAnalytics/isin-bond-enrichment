@echo off
chcp 65001 >nul 2>nul
title ISIN Bond Enrichment Terminal
color 0A

echo.
echo   _____ _____ _____ _   _
echo  ^|_   _^|  ___^|_   _^| \ ^| ^|
echo    ^| ^| ^| ^|___  ^| ^| ^|  \^| ^|
echo    ^| ^| ^|___  ^| ^| ^| ^| . ` ^|
echo   _^| ^|_ ___^| ^|_^| ^|_^| ^|\  ^|
echo  ^|_____^|_____^|_____^|_^| \_^|
echo.
echo   Bond Enrichment Terminal
echo   ________________________
echo.
echo   Preparing your environment...
echo.

cd /d "%~dp0"
if errorlevel 1 goto :err_directory

setlocal enabledelayedexpansion

set PYTHON=

where py >nul 2>nul
if not errorlevel 1 (
    set PYTHON=py
    goto :found_python
)

where python >nul 2>nul
if not errorlevel 1 (
    set PYTHON=python
    goto :found_python
)

where python3 >nul 2>nul
if not errorlevel 1 (
    set PYTHON=python3
    goto :found_python
)

goto :err_no_python

:found_python
echo   [Step 1 of 4]  Python found
echo.
echo                  Command:  %PYTHON%
for /f "tokens=*" %%v in ('%PYTHON% --version 2^>^&1') do echo                  Version:  %%v
echo.

%PYTHON% -c "import sys; ver=sys.version_info; exit(0 if ver[0]>=3 and ver[1]>=9 else 1)" 2>nul
if errorlevel 1 goto :err_python_version

if exist ".venv\Scripts\activate.bat" goto :venv_ready

echo   [Step 2 of 4]  Creating virtual environment...
echo.
echo                  This only happens on first run.
echo.
%PYTHON% -m venv .venv
if errorlevel 1 goto :err_venv
echo                  Virtual environment created.
echo.
goto :venv_activate

:venv_ready
echo   [Step 2 of 4]  Virtual environment ready
echo.

:venv_activate
call .venv\Scripts\activate.bat

python -c "import fastapi" >nul 2>nul
if not errorlevel 1 goto :deps_ready

echo   [Step 3 of 4]  Installing dependencies...
echo.
echo                  This may take a minute...
echo.

set INSTALL_FAILED=0
set /a TOTAL=8
set /a CURRENT=0

call :install_pkg fastapi
call :install_pkg "uvicorn[standard]"
call :install_pkg pandas
call :install_pkg openpyxl
call :install_pkg requests
call :install_pkg websockets
call :install_pkg python-multipart
call :install_pkg pywin32

echo.
if "!INSTALL_FAILED!"=="1" goto :err_deps
echo                  All packages installed successfully.
echo.
goto :launch

:deps_ready
echo   [Step 3 of 4]  Dependencies already installed
echo.

:launch
echo   [Step 4 of 4]  Starting server...
echo.
echo   +---------------------------------------------------+
echo   ^|                                                   ^|
echo   ^|   ISIN Bond Enrichment Terminal is running!       ^|
echo   ^|                                                   ^|
echo   ^|   Open your browser to:                           ^|
echo   ^|                                                   ^|
echo   ^|      ^>^>^> http://localhost:8000 ^<^<^<                 ^|
echo   ^|                                                   ^|
echo   ^|   Press Ctrl+C to stop the server.                ^|
echo   ^|                                                   ^|
echo   +---------------------------------------------------+
echo.

start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8000"

python server.py

echo.
echo   Server stopped.
pause
goto :eof

:err_directory
echo.
echo   ERROR: Cannot change to the script directory.
echo   Please copy this folder to a local drive and run it from there.
echo.
pause
exit /b 1

:err_no_python
echo.
echo   ERROR: Python not found.
echo   Please install Python 3.9+ from https://www.python.org/downloads/
echo   IMPORTANT: Check "Add Python to PATH" during installation.
echo.
pause
exit /b 1

:err_python_version
echo.
echo   ERROR: Python 3.9 or higher is required.
echo   Please upgrade from python.org/downloads
echo.
pause
exit /b 1

:err_venv
echo.
echo   ERROR: Failed to create virtual environment.
echo   Try running: %PYTHON% -m ensurepip
pause
exit /b 1

:err_deps
echo   ERROR: One or more packages failed to install.
echo   Check your internet connection and try again.
pause
exit /b 1

:install_pkg
set /a CURRENT+=1
set "PKG=%~1"

set "BAR="
set /a FILLED=CURRENT*10/TOTAL
set /a EMPTY=10-FILLED
call :build_bar
<nul set /p =     [!BAR!] !CURRENT!/!TOTAL!  Installing !PKG! ...

pip install "!PKG!" --quiet --disable-pip-version-check --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org >"%TEMP%\pip_out.txt" 2>&1
if errorlevel 1 (
    echo  FAILED
    echo.
    echo   ---- pip error output ----
    type "%TEMP%\pip_out.txt"
    echo   --------------------------
    echo.
    set INSTALL_FAILED=1
) else (
    echo  done
)
exit /b 0

:build_bar
set "BAR="
for /l %%i in (1,1,%FILLED%) do set "BAR=!BAR!#"
for /l %%i in (1,1,%EMPTY%)  do set "BAR=!BAR!."
exit /b 0
