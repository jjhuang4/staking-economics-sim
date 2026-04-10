import json
import os
import re
import subprocess

def check_requirements():
    with open('requirements.txt') as f:
        lines = f.read().splitlines()
    
    for line in lines:
        line = line.split('#')[0].strip()
        if not line:
            continue
        package = re.split(r'[=<>!~]+', line)[0].strip()
        try:
            __import__(package)
            print(f"Package '{package}' is already installed.")
        except ImportError:
            print(f"Package '{package}' is not installed. Installing...")
            subprocess.check_call([os.sys.executable, '-m', 'pip', 'install', line])

if __name__ == "__main__":
    check_requirements()
    with open('output/output.txt', 'w') as f:
        f.write('All requirements checked and installed successfully.\n')