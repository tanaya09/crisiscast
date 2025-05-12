from fastapi import FastAPI
from pydantic import BaseModel
from transformers import T5Tokenizer, T5ForConditionalGeneration
import torch
import re

app = FastAPI()

# Load model and tokenizer from local directory
model_dir = "models/flan-t5-base"
tokenizer = T5Tokenizer.from_pretrained(model_dir)
tokenizer.pad_token = tokenizer.eos_token
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"🚀 Using device: {device} ({torch.cuda.get_device_name(device) if torch.cuda.is_available() else 'CPU'})")
model = T5ForConditionalGeneration.from_pretrained(model_dir).to(device)
model.eval()

# Valid crisis classes
crisis_labels = [
    "natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war",
    "financial_crisis", "civil_unrest", "infrastructure_failure", "environmental_crisis", "crime", "none"
]

# Prompt format
def format_prompt(text: str) -> str:
    return (
        "Many texts are not about crises. If the text is unrelated to any crisis, respond with: none.\n\n"
        "Classify the type of crisis described below in one word. "
        "Choose from: natural_disaster, terrorist_attack, cyberattack, pandemic, war, "
        "financial_crisis, civil_unrest, infrastructure_failure, environmental_crisis, crime, none.\n\n"
        f"Text: {text.strip()}"
    )

# Clean model output
def clean_label(output: str) -> str:
    output = output.lower()
    matches = re.findall(
        r"(natural_disaster|terrorist_attack|cyberattack|pandemic|war|financial_crisis|civil_unrest|infrastructure_failure|environmental_crisis|crime|none)",
        output
    )
    return matches[0] if matches else "none"

# Input format
class BatchInput(BaseModel):
    inputs: list[str]

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/classify_batch")
def classify_batch(data: BatchInput):
    prompts = [format_prompt(text) for text in data.inputs]
    inputs = tokenizer(prompts, return_tensors="pt", padding=True, truncation=True).to(device)
    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=6)
    labels = [clean_label(tokenizer.decode(o, skip_special_tokens=True)) for o in outputs]
    print(labels)
    return {"labels": labels}
