# Predictive Maintenance Summary Generator

This script processes technician work orders in batches and produces JSON summaries for predictive maintenance.

## 🔧 How to Use

1. Place your CSV in `data/sample_data.csv`.
2. Run the script:
   ```bash
   python generate_summaries.py
   ```
3. Summaries will be saved to the `output/` folder.

## 📁 Output Format

Each file is a structured JSON that includes:
- Critical assets at risk
- Estimated days to failure
- Downtime risk scores
- Root causes and recommendations

## ✅ Requirements

- Python 3.x
- pandas

Install dependencies:
```bash
pip install pandas
```
