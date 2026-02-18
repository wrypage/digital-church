#!/bin/bash
# Scheduled vacuum run - executes after YouTube API quota reset (midnight PT)

cd /Users/stephenmorse/Downloads/digital-church

# Log file for the scheduled run
LOG_FILE="logs/vacuum_scheduled_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

echo "=== Scheduled Vacuum Run ===" | tee -a "$LOG_FILE"
echo "Started at: $(date)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Run vacuum
python3 run.py vacuum 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Completed at: $(date)" | tee -a "$LOG_FILE"
