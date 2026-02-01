import requests
from bs4 import BeautifulSoup
import json
import time
import boto3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Telugu + English mapping
RASI_URLS = [
    {"en": "Aries", "te": "మేష రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/mesha-rasi-phalalu.asp"},
    {"en": "Taurus", "te": "వృషభ రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/vrusha-rasi-phalalu.asp"},
    {"en": "Gemini", "te": "మిథున రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/mithuna-rasi-phalalu.asp"},
    {"en": "Cancer", "te": "కర్కాటక రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/karkataka-rasi-phalalu.asp"},
    {"en": "Leo", "te": "సింహ రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/simha-rasi-phalalu.asp"},
    {"en": "Virgo", "te": "కన్య రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/kanya-rasi-phalalu.asp"},
    {"en": "Libra", "te": "తుల రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/tula-rasi-phalalu.asp"},
    {"en": "Scorpio", "te": "వృశ్చిక రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/vrushchika-rasi-phalalu.asp"},
    {"en": "Sagittarius", "te": "ధనుస్సు రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/dhanusu-rasi-phalalu.asp"},
    {"en": "Capricorn", "te": "మకర రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/makara-rasi-phalalu.asp"},
    {"en": "Aquarius", "te": "కుంభ రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/kumbha-rasi-phalalu.asp"},
    {"en": "Pisces", "te": "మీనం రాశి", "url": "https://www.astrosage.com/telugu/rasi-phalalu/weekly/meena-rasi-phalalu.asp"},
]

BUCKET_NAME = "sahasra2026week"
S3_KEY = "telcal_Week.json"

def clean_text(text):
    return "\n".join([line.strip() for line in text.split("\n") if line.strip()])

def scrape_rasi(item):
    print(f"Fetching {item['te']}...")

    r = requests.get(item["url"], headers=HEADERS, timeout=20)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Week text
    week_text = ""
    for tag in soup.find_all(["h2", "h3"]):
        if "202" in tag.get_text():
            week_text = tag.get_text(strip=True)
            break

    content_div = soup.select_one("div.ui-padding-all.ui-large-content.text-justify")
    if not content_div:
        print(f"⚠ Content not found for {item['te']}")
        return None

    body = clean_text(content_div.get_text(separator="\n", strip=True))

    description = (
        f"{week_text}\n"
        f"{item['te']} ఫలాలు\n"
        f"{body}"
    )

    return {
        item["en"]: {
            "description": description
        }
    }

def upload_to_s3(data):
    s3 = boto3.client("s3", region_name="ap-south-1")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_KEY,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )

def main():
    output = []

    for item in RASI_URLS:
        try:
            data = scrape_rasi(item)
            if data:
                output.append(data)
            time.sleep(2)
        except Exception as e:
            print(f"❌ Error for {item['te']}: {e}")

    upload_to_s3(output)
    print("✅ Weekly horoscope JSON uploaded to S3")

if __name__ == "__main__":
    main()
