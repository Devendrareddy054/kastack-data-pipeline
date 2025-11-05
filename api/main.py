from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(title="Customer Data API")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CSV_FILE = os.path.join(DATA_DIR, "customers.csv")

@app.get("/")
def root():
    return {"message": "Go to /customers for data"}

@app.get("/customers")
def get_customers():
    try:
        df = pd.read_csv(CSV_FILE)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
