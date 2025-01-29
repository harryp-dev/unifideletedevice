#!/usr/bin/env python3
import pymongo
from pymongo import MongoClient
import pprint
import subprocess
import datetime
import os
import shutil

# Get user input with clear validation hints
MONGO_HOST = input("Enter MongoDB server IP or hostname: ")
device_mac = input("Enter device MAC address (lowercase, no separators): ")

# Backup configuration with timestamped directories
backup_parent_dir = 'backups'  # Main backup storage location
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")  # Filename-safe format
backup_dir = os.path.join(backup_parent_dir, timestamp)  # Full backup path

try:
    # Create nested backup directory (no error if exists due to recent timestamp)
    os.makedirs(backup_dir, exist_ok=True)
    print(f"Initializing backup in: {backup_dir}")

    # Build mongodump command with explicit parameters
    mongodump_cmd = [
        'mongodump',
        '--host', MONGO_HOST,
        '--port', '27117',  # Non-default MongoDB port
        '--db', 'ace',      # Target database name
        '--out', backup_dir # Output directory for dump
    ]

    # Execute backup with real-time output capture
    result = subprocess.run(
        mongodump_cmd,
        check=True,          # Raise exception on failure
        capture_output=True, # Capture both stdout and stderr
        text=True            # Return output as strings (not bytes)
    )
    print("Backup completed successfully")

except subprocess.CalledProcessError as e:
    print(f"Backup failed - {e.stderr}")
    shutil.rmtree(backup_dir, ignore_errors=True)
    exit(1)
except Exception as e:
    print(f"Unexpected backup error: {str(e)}")
    shutil.rmtree(backup_dir, ignore_errors=True)
    exit(1)

# Backup verification checks
try:
    # Verify database dump structure
    ace_dump_path = os.path.join(backup_dir, 'ace')
    if not os.path.isdir(ace_dump_path):
        raise Exception("Missing database directory in backup")

    # Confirm target collection exists in backup
    device_dump_file = os.path.join(ace_dump_path, 'device.bson')
    if not os.path.isfile(device_dump_file):
        raise Exception("Target collection not found in backup")

    print(f"Backup verified: {device_dump_file} exists")

except Exception as e:
    print(f"Verification failed: {str(e)}")
    print("Removing invalid backup...")
    shutil.rmtree(backup_dir, ignore_errors=True)
    exit(1)

# Database modification with modern delete operation
try:
    client = MongoClient(MONGO_HOST, 27117)
    db = client['ace']
    
    # Use delete_one for single document removal (vs deprecated remove())
    result = db.device.delete_one({'mac': device_mac})
    
    print("\nDeletion results:")
    pprint.pprint(result.raw_result)  # MongoDB response document
    print(f"Deleted {result.deleted_count} document(s)")

except Exception as e:
    print(f"Database operation failed: {str(e)}")
    exit(1)