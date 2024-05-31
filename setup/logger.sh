#!/bin/bash

# Define color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display output messages without color
output() {
  echo -e "${NC}$1${NC}"
}

# Function to print timestamp
timestamp() {
  date +"%a %d-%b-%Y %I:%M:%S.%3N %p %Z"
  #date +"%a %d-%b-%Y %r %Z"
}

command_output_with_timestamp() {
  echo -e "$(timestamp) | ${NC}'$1'${NC}"
  while IFS= read -r line; do
    echo -e "$(timestamp) | ${NC}$line${NC}"
  done < <($1)
}

# Function to run commands with error checking and timestamped output
run_command() {
    local command="$@"
    info "Running command: '$command'"

    # Capture the command output with timestamp
#    while IFS= read -r line; do
#        output_with_timestamp "\t${NC}$line${NC}"
#    done < <($command)

    {
        eval "$command" 2>&1
        echo $? > /tmp/command_exit_status
    } | while IFS= read -r line; do
         output_with_timestamp "\t${NC}$line${NC}"
    done

    # Get the exit code of the command
#    EXIT_CODE=${PIPESTATUS[0]}
#    if [ "$EXIT_CODE" -ne 0 ]; then
#        error "Command '$command' failed with exit code '$EXIT_CODE'"
#        exit "$EXIT_CODE"
#    fi

    # Get the exit code of the command
    EXIT_CODE=$(cat /tmp/command_exit_status)
    rm /tmp/command_exit_status

    if [ "$EXIT_CODE" -ne 0 ]; then
        error "Command '$command' failed with exit code '$EXIT_CODE'"
        exit "$EXIT_CODE"
    fi
}


output_with_timestamp() {
  echo -e "$(timestamp) | ${NC}OUTPUT${NC} | ${NC}$1${NC}"
}

error() {
  echo -e "$(timestamp) | ${RED}ERROR${NC} | $1"
}

# Function to display info messages in green
trace() {
  echo -e "$(timestamp) | ${BLUE}TRACE${NC} | $1"
}

# Function to display info messages in green
info() {
  echo -e "$(timestamp) | ${GREEN}INFO${NC} | $1"
}

# Function to display debug messages in cyan
debug() {
  echo -e "$(timestamp) | ${CYAN}DEBUG${NC} | $1"
}

# Function to display warn messages in yellow
warn() {
  echo -e "$(timestamp) | ${YELLOW}WARN${NC} | $1"
}

# Usage examples (uncomment to test):
#info "This is an info message"
#debug "This is a debug message"
#warn "This is a warning message"
#trace "This is a trace message"
#error "This is an error message"
