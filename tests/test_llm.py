import requests
import os
from dotenv import load_dotenv

# Load your Hugging Face API key
load_dotenv("config/.env")
API_TOKEN = os.getenv("HF_API_KEY")

API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-base"
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}

def query(payload):
    response = requests.post(API_URL, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"Error: {response.status_code}", response.text)
        return None
    return response.json()

# Example crisis texts
texts = [
    "Explosion reported in downtown Manhattan.",
    "Wildfire spreads across California.",
    "Microsoft releases new Windows update.",
    "Flood warnings issued after heavy rains in Mumbai.",
    "Peace talks initiated between two countries."
]

for text in texts:
    prompt = f"Classify the type of crisis in the following sentence:\n{text}\nCrisis type:"
    output = query({"inputs": prompt})
    
    if output:
        result = output[0]["generated_text"]
        print(f"Text: {text}\nCrisis Type: {result}\n")
