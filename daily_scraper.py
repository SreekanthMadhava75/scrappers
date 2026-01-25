# import requests
# from bs4 import BeautifulSoup
# import json
# import time
# import boto3
# from datetime import datetime

# HEADERS = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
# }

# RASI_URLS = [
#     ("మేష రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/mesha-rasi-phalalu.asp"),
#     ("వృషభ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/vrusha-rasi-phalalu.asp"),
#     ("మిథున రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/mithuna-rasi-phalalu.asp"),
#     ("కర్కాటక రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/karkataka-rasi-phalalu.asp"),
#     ("సింహ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/simha-rasi-phalalu.asp"),
#     ("కన్య రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/kanya-rasi-phalalu.asp"),
#     ("తుల రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/tula-rasi-phalalu.asp"),
#     ("వృశ్చిక రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/vrushchika-rasi-phalalu.asp"),
#     ("ధనుస్సు రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/dhanusu-rasi-phalalu.asp"),
#     ("మకర రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/makara-rasi-phalalu.asp"),
#     ("కుంభ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/kumbha-rasi-phalalu.asp"),
#     ("మీనం రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/meena-rasi-phalalu.asp")
# ]

# def clean_text(text):
#     return "\n".join([line.strip() for line in text.split("\n") if line.strip()])

# def scrape_data(rasi_name, url):
#     print(f"Fetching {rasi_name}...")

#     response = requests.get(url, headers=HEADERS, timeout=20)
#     response.raise_for_status()

#     soup = BeautifulSoup(response.text, "html.parser")

#     # Date / day text
#     date_text = ""
#     for tag in soup.find_all(["h2", "h3"]):
#         if any(char.isdigit() for char in tag.get_text()):
#             date_text = tag.get_text(strip=True)
#             break

#     content_div = soup.select_one("div.ui-large-content.text-justify")

#     if not content_div:
#         return None

#     body_text = clean_text(
#         content_div.get_text(separator="\n", strip=True)
#     )

#     final_description = (
#         f"{rasi_name} – Daily Horoscope in Telugu\n"
#         f"{date_text}\n\n"
#         f"{body_text}"
#     )

#     return {
#         "description": final_description
#     }

# def upload_to_s3(data):
#     s3 = boto3.client("s3")
#     s3.put_object(
#         Bucket="sahasraday2026",
#         Key="telcal_Day.json",
#         Body=json.dumps(data, ensure_ascii=False, indent=2),
#         ContentType="application/json"
#     )

# def main():
#     result = []

#     for rasi_name, url in RASI_URLS:
#         try:
#             data = scrape_data(rasi_name, url)
#             if data:
#                 result.append(data)
#             time.sleep(2)
#         except Exception as e:
#             print(f"Error for {rasi_name}: {e}")

#     upload_to_s3(result)
#     print("✅ Daily horoscope JSON uploaded to S3")

# if __name__ == "__main__":
#     main()

import requests
from bs4 import BeautifulSoup
import json
import time
import boto3
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Telugu → English Rasi mapping
RASI_URLS = [
    ("Aries", "మేష రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/mesha-rasi-phalalu.asp"),
    ("Taurus", "వృషభ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/vrusha-rasi-phalalu.asp"),
    ("Gemini", "మిథున రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/mithuna-rasi-phalalu.asp"),
    ("Cancer", "కర్కాటక రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/karkataka-rasi-phalalu.asp"),
    ("Leo", "సింహ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/simha-rasi-phalalu.asp"),
    ("Virgo", "కన్య రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/kanya-rasi-phalalu.asp"),
    ("Libra", "తుల రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/tula-rasi-phalalu.asp"),
    ("Scorpio", "వృశ్చిక రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/vrushchika-rasi-phalalu.asp"),
    ("Sagittarius", "ధనుస్సు రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/dhanusu-rasi-phalalu.asp"),
    ("Capricorn", "మకర రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/makara-rasi-phalalu.asp"),
    ("Aquarius", "కుంభ రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/kumbha-rasi-phalalu.asp"),
    ("Pisces", "మీనం రాశి", "https://www.astrosage.com/telugu/rasi-phalalu/meena-rasi-phalalu.asp")
]

def clean_text(text):
    return "\n".join(line.strip() for line in text.split("\n") if line.strip())

def scrape_data(eng_name, tel_name, url):
    print(f"Fetching {tel_name}...")

    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract date line (if available)
    date_text = ""
    for tag in soup.find_all(["h2", "h3"]):
        if any(char.isdigit() for char in tag.get_text()):
            date_text = tag.get_text(strip=True)
            break

    content_div = soup.select_one("div.ui-large-content.text-justify")
    if not content_div:
        return None

    body_text = clean_text(content_div.get_text(separator="\n"))

    today = datetime.now().strftime("%d %B %Y")

    description = (
        f"{tel_name} – Daily Horoscope in Telugu\n"
        f"Date: {today}\n"
        f"{date_text}\n\n"
        f"{body_text}"
    )

    # ✅ REQUIRED JSON STRUCTURE
    return {
        eng_name: {
            "description": description
        }
    }

def upload_to_s3(data):
    s3 = boto3.client("s3", region_name="ap-south-1")
    s3.put_object(
        Bucket="sahasraday2026",
        Key="telcal_Day.json",
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )

def main():
    result = []

    for eng, tel, url in RASI_URLS:
        try:
            data = scrape_data(eng, tel, url)
            if data:
                result.append(data)
            time.sleep(2)
        except Exception as e:
            print(f"Error for {tel}: {e}")

    upload_to_s3(result)
    print("✅ Daily horoscope JSON uploaded to S3 in correct format")

if __name__ == "__main__":
    main()

