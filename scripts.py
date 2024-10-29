# scripts.py
import subprocess


def run_formatters():
    subprocess.run(['isort', '.'])
    subprocess.run(['blue', '.'])
