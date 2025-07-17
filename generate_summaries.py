
import pandas as pd
import json
import os

# === Settings ===
INPUT_CSV = "data/sample_data.csv"
OUTPUT_DIR = "output"
BATCH_SIZE = 10

# === Create output directory ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Load CSV ===
df = pd.read_csv(INPUT_CSV)

# === Helper Functions ===

def estimate_days_to_failure(age_months, last_maintenance_days, priority):
    if priority.lower() == "critical":
        base = 7
    elif priority.lower() == "high":
        base = 14
    elif priority.lower() == "medium":
        base = 21
    else:
        base = 30
    modifier = (age_months / 60) + (last_maintenance_days / 180)
    return max(3, int(base / (modifier + 0.1)))

def compute_downtime_risk_score(age_months, downtime_hours, priority):
    score = (age_months / 100) * 3 + (downtime_hours / 5) * 3
    score += {"Low": 1, "Medium": 2, "High": 3, "Critical": 4}.get(priority, 1)
    return min(round(score, 1), 10.0)

# === Split into batches ===
batches = [df[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]

# === Process Each Batch ===
for idx, batch in enumerate(batches):
    critical_assets = []

    for _, row in batch.iterrows():
        days_to_failure = estimate_days_to_failure(
            row['equipment_age_months'],
            row['last_maintenance_days'],
            row['priority']
        )

        risk_score = compute_downtime_risk_score(
            row['equipment_age_months'],
            row['downtime_hours'],
            row['priority']
        )

        if row['priority'] in ['High', 'Critical']:
            critical_assets.append({
                "asset_id": row['asset_id'],
                "asset_type": row['asset_type'],
                "location": row['asset_location'],
                "estimated_days_to_failure": days_to_failure,
                "failure_confidence": round(min(0.95, 0.7 + risk_score / 20), 2),
                "root_cause": row['root_cause'],
                "last_maintenance_days": row['last_maintenance_days'],
                "equipment_age_months": row['equipment_age_months'],
                "downtime_risk_score": risk_score,
                "recommendation": f"Check asset within {max(1, days_to_failure // 2)} days"
            })

    # Global Insights
    root_causes = batch['root_cause'].value_counts().head(3).index.tolist()
    priorities = batch['priority'].value_counts().to_dict()
    avg_days_to_failure = (
        sum([a['estimated_days_to_failure'] for a in critical_assets]) / len(critical_assets)
        if critical_assets else 0
    )

    summary = {
        "batch_id": idx + 1,
        "summary": {
            "critical_assets_at_risk": critical_assets,
            "global_insights": {
                "most_common_root_causes": root_causes,
                "average_days_to_next_failure": round(avg_days_to_failure, 2),
                "total_assets_at_risk": len(critical_assets),
                "priority_distribution": priorities
            },
            "actionable_recommendations": [
                "Increase inspection frequency for assets >50 months old",
                "Automate alerts for lubrication cycles >60 days",
                "Pre-stock parts for frequently failing asset types"
            ]
        }
    }

    output_path = os.path.join(OUTPUT_DIR, f"predictive_summary_batch_{idx + 1}.json")
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Saved: {output_path}")
