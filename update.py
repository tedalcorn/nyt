#!/usr/bin/env python3
"""
Update script: fetch any new months from NYT API, then rebuild dashboard data.

Usage:
  python update.py           # Fetch new + rebuild
  python update.py --rebuild # Just rebuild from existing raw data
"""

import sys
import subprocess

def main():
    if "--rebuild" not in sys.argv:
        print("=== Fetching new data from NYT Archive API ===")
        subprocess.run([sys.executable, "fetch_nyt.py"], check=True)

    print("\n=== Building dashboard data ===")
    subprocess.run([sys.executable, "build_data.py"], check=True)

    print("\nDone! Serve the dashboard with: python3 -m http.server 8080")

if __name__ == "__main__":
    main()
