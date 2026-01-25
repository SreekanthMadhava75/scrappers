import requests
import os

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
OWNER = "SreekanthMadhava75"
REPO = "scrappers"

url = f"https://api.github.com/repos/{OWNER}/{REPO}/dispatches"

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

data = {
    "event_type": "daily-cron"
}

r = requests.post(url, headers=headers, json=data)
print("Triggered:", r.status_code)
