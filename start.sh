#!/bin/bash

# ── Colors ──
BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
DIM='\033[2m'
RESET='\033[0m'

clear

echo ""
echo -e "${CYAN}${BOLD}"
echo "   _____ _____ _____ _   _ "
echo "  |_   _|  ___|_   _| \ | |"
echo "    | | | |___  | | |  \| |"
echo "    | | |___  | | | | . \` |"
echo "   _| |_ ___| |_| |_| |\  |"
echo "  |_____|_____|_____|_| \_|"
echo ""
echo -e "${RESET}${BOLD}   Bond Enrichment Terminal${RESET}"
echo -e "   ${DIM}________________________${RESET}"
echo ""
echo -e "   Preparing your environment..."
echo ""

# Navigate to script directory
cd "$(dirname "$0")"

# ══════════════════════════════════════════════════════════
# STEP 1 — Find Python (try python3, python, python3.x)
# ══════════════════════════════════════════════════════════
PYTHON=""

# Try python3 first (preferred on macOS/Linux)
if command -v python3 &> /dev/null; then
    PYTHON=python3
# Try python
elif command -v python &> /dev/null; then
    # Verify it's Python 3, not Python 2
    PY_MAJOR=$(python -c "import sys; print(sys.version_info[0])" 2>/dev/null)
    if [ "$PY_MAJOR" = "3" ]; then
        PYTHON=python
    fi
fi

# Try versioned python3.x (3.14 down to 3.9)
if [ -z "$PYTHON" ]; then
    for ver in 14 13 12 11 10 9; do
        if command -v "python3.${ver}" &> /dev/null; then
            PYTHON="python3.${ver}"
            break
        fi
    done
fi

# Try 'py' launcher (sometimes available on Windows WSL)
if [ -z "$PYTHON" ]; then
    if command -v py &> /dev/null; then
        PYTHON="py -3"
    fi
fi

if [ -z "$PYTHON" ]; then
    echo ""
    echo -e "   ${RED}${BOLD}+---------------------------------------------------+${RESET}"
    echo -e "   ${RED}${BOLD}|  ERROR: Python not found                          |${RESET}"
    echo -e "   ${RED}${BOLD}|                                                   |${RESET}"
    echo -e "   ${RED}${BOLD}|  Please install Python 3.9+ from:                 |${RESET}"
    echo -e "   ${RED}${BOLD}|  https://www.python.org/downloads/                |${RESET}"
    echo -e "   ${RED}${BOLD}|                                                   |${RESET}"
    echo -e "   ${RED}${BOLD}|  macOS:  brew install python3                     |${RESET}"
    echo -e "   ${RED}${BOLD}|  Ubuntu: sudo apt install python3 python3-venv    |${RESET}"
    echo -e "   ${RED}${BOLD}+---------------------------------------------------+${RESET}"
    echo ""
    read -p "   Press Enter to exit..."
    exit 1
fi

PY_VERSION=$($PYTHON --version 2>&1)

echo -e "   ${GREEN}[Step 1 of 4]${RESET}  Python found"
echo ""
echo -e "                  Command:  ${BOLD}$PYTHON${RESET}"
echo -e "                  Version:  ${BOLD}$PY_VERSION${RESET}"
echo ""

# ══════════════════════════════════════════════════════════
# STEP 2 — Verify Python version >= 3.9
# ══════════════════════════════════════════════════════════
$PYTHON -c "import sys; exit(0 if sys.version_info >= (3,9) else 1)" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "   ${RED}${BOLD}ERROR: Python 3.9 or higher is required.${RESET}"
    echo -e "   ${RED}Please upgrade from python.org/downloads${RESET}"
    echo ""
    read -p "   Press Enter to exit..."
    exit 1
fi

# ══════════════════════════════════════════════════════════
# STEP 3 — Create virtual environment
# ══════════════════════════════════════════════════════════
if [ ! -d ".venv" ]; then
    echo -e "   ${GREEN}[Step 2 of 4]${RESET}  Creating virtual environment..."
    echo ""
    echo -e "                  ${DIM}This only happens on first run.${RESET}"
    echo ""
    $PYTHON -m venv .venv
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "   ${RED}ERROR: Failed to create virtual environment.${RESET}"
        echo -e "   ${DIM}On Ubuntu/Debian, try: sudo apt install python3-venv${RESET}"
        echo ""
        read -p "   Press Enter to exit..."
        exit 1
    fi
    echo -e "                  ${GREEN}Virtual environment created.${RESET}"
    echo ""
else
    echo -e "   ${GREEN}[Step 2 of 4]${RESET}  Virtual environment ready"
    echo ""
fi

# Activate
source .venv/bin/activate

# ══════════════════════════════════════════════════════════
# STEP 4 — Install dependencies
# ══════════════════════════════════════════════════════════
python -c "import fastapi" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "   ${GREEN}[Step 3 of 4]${RESET}  Installing dependencies..."
    echo ""
    echo -e "                  ${DIM}fastapi, uvicorn, pandas, openpyxl,${RESET}"
    echo -e "                  ${DIM}requests, websockets, python-multipart${RESET}"
    echo ""
    pip install -r requirements.txt --quiet --disable-pip-version-check 2>/dev/null
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "   ${RED}ERROR: Failed to install dependencies.${RESET}"
        echo -e "   ${DIM}Check your internet connection and try again.${RESET}"
        echo ""
        read -p "   Press Enter to exit..."
        exit 1
    fi
    echo -e "                  ${GREEN}All packages installed.${RESET}"
    echo ""
else
    echo -e "   ${GREEN}[Step 3 of 4]${RESET}  Dependencies already installed"
    echo ""
fi

# ══════════════════════════════════════════════════════════
# STEP 5 — Launch
# ══════════════════════════════════════════════════════════
echo -e "   ${GREEN}[Step 4 of 4]${RESET}  Starting server..."
echo ""
echo -e "   ${CYAN}${BOLD}+---------------------------------------------------+${RESET}"
echo -e "   ${CYAN}${BOLD}|                                                   |${RESET}"
echo -e "   ${CYAN}${BOLD}|   ISIN Bond Enrichment Terminal is running!       |${RESET}"
echo -e "   ${CYAN}${BOLD}|                                                   |${RESET}"
echo -e "   ${CYAN}${BOLD}|   Open your browser to:                           |${RESET}"
echo -e "   ${CYAN}${BOLD}|                                                   |${RESET}"
echo -e "   ${CYAN}${BOLD}|      ${YELLOW}>>> http://localhost:8000 <<<${CYAN}                 |${RESET}"
echo -e "   ${CYAN}${BOLD}|                                                   |${RESET}"
echo -e "   ${CYAN}${BOLD}|   Press Ctrl+C to stop the server.                |${RESET}"
echo -e "   ${CYAN}${BOLD}|                                                   |${RESET}"
echo -e "   ${CYAN}${BOLD}+---------------------------------------------------+${RESET}"
echo ""

# Open browser after short delay (works on macOS, Linux, WSL)
(
    sleep 2
    if command -v open &> /dev/null; then
        open "http://localhost:8000"
    elif command -v xdg-open &> /dev/null; then
        xdg-open "http://localhost:8000"
    elif command -v wslview &> /dev/null; then
        wslview "http://localhost:8000"
    elif command -v sensible-browser &> /dev/null; then
        sensible-browser "http://localhost:8000"
    fi
) &

# Start server
python server.py

echo ""
echo -e "   ${DIM}Server stopped.${RESET}"
