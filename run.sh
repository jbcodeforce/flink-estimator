#!/bin/bash
#
# Flink Resource Estimator - Run Script
# 
# This script starts the FastAPI application using the namespaced package structure.
# It automatically navigates to the src directory and runs the app with uvicorn.
#

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Print banner
echo -e "${BLUE}"
echo "🚀 Flink Resource Estimator"
echo "================================"
echo -e "${NC}"

# Check if we're in the right directory
if [ ! -d "src" ]; then
    print_error "src directory not found. Please run this script from the repository root."
    exit 1
fi

if [ ! -d "src/flink_estimator" ]; then
    print_error "flink_estimator package not found in src/. Package structure may be incorrect."
    exit 1
fi

# Check if uv is available
if ! command -v uv &> /dev/null; then
    print_error "uv is not installed or not in PATH. Please install uv first."
    print_info "Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Navigate to src directory
print_info "Navigating to src directory..."
cd src

# Check if virtual environment exists, if not create it
if [ ! -d ".venv" ]; then
    print_info "Virtual environment not found. Creating one..."
    uv venv
    print_success "Virtual environment created."
fi

# Install dependencies
print_info "Installing dependencies..."
uv pip install -r requirements.txt

# Default values
HOST=${HOST:-"0.0.0.0"}
PORT=${PORT:-8000}
RELOAD=${RELOAD:-"--reload"}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --no-reload)
            RELOAD=""
            shift
            ;;
        --prod|--production)
            RELOAD=""
            print_warning "Running in production mode (no auto-reload)"
            shift
            ;;
        --test)
            print_info "Running unit tests..."
            cd src
            uv run pytest tests/ -v
            exit $?
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --host HOST       Host to bind to (default: 0.0.0.0)"
            echo "  --port PORT       Port to bind to (default: 8000)"
            echo "  --no-reload       Disable auto-reload"
            echo "  --prod            Production mode (no auto-reload)"
            echo "  --test            Run unit tests and exit"
            echo "  -h, --help        Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  HOST              Host to bind to"
            echo "  PORT              Port to bind to"
            echo "  RELOAD            Set to empty to disable reload"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            print_info "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Start the application
print_success "Starting Flink Resource Estimator..."
print_info "Host: $HOST"
print_info "Port: $PORT"
print_info "Access the app at: http://$HOST:$PORT"
print_info "API docs at: http://$HOST:$PORT/docs"
echo ""

if [ -n "$RELOAD" ]; then
    print_info "Running in development mode with auto-reload enabled"
    uv run uvicorn flink_estimator.main:app --host "$HOST" --port "$PORT" $RELOAD
else
    print_info "Running in production mode"
    uv run uvicorn flink_estimator.main:app --host "$HOST" --port "$PORT"
fi