import os
import openai
import pandas as pd
import json
from dotenv import load_dotenv
from pathlib import Path

# Load API key
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Constants
INPUT_CSV = "data/sample_data.csv"
BATCH_SIZE = 10
OUTPUT_FOLDER = "output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Read the CSV
df = pd.read_csv(INPUT_CSV)

def chunk_dataframe(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i + size]

def generate_prompt(csv_chunk):
    return f"""
You are a maintenance analyst.

Given this technician work order data (in CSV row format), generate a JSON array. Each object must include:

- asset_id
- asset_type
- failure_in: human-readable estimate like "within 3 months"
- reason_for_failure: inferred from the problem_description and history
- recommended_action: one action based on the cause

DO NOT repeat the same recommendation across assets.

Here is the data (columns may include: asset_id, asset_type, problem_description, etc):

{csv_chunk.to_csv(index=False)}
"""

def summarize_batch(batch_df, batch_number):
    prompt = generate_prompt(batch_df)

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # You can change to "gpt-3.5-turbo" if needed
            messages=[
                {"role": "system", "content": "You are a helpful AI assistant for predictive maintenance insights."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=1000
        )

        summary_json = response["choices"][0]["message"]["content"]

        # Save each batch summary
        output_path = Path(OUTPUT_FOLDER) / f"predictive_summary_batch_{batch_number}.json"
        with open(output_path, "w") as f:
            f.write(summary_json)

        return summary_json

    except Exception as e:
        print(f"Error in batch {batch_number}: {e}")
        return None

# Process all batches
batch_outputs = []
for i, chunk in enumerate(chunk_dataframe(df, BATCH_SIZE), start=1):
    print(f"Processing batch {i}...")
    batch_output = summarize_batch(chunk, i)
    if batch_output:
        batch_outputs.append(batch_output)

# Combine all batch outputs into master summary
all_assets = []
for batch_json in batch_outputs:
    try:
        data = json.loads(batch_json)
        all_assets.extend(data)
    except Exception as e:
        print("Error parsing batch JSON:", e)

master_path = Path(OUTPUT_FOLDER) / "master_summary.json"
with open(master_path, "w") as f:
    json.dump(all_assets, f, indent=4)

print("All summaries generated!")
