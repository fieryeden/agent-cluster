import sqlite3
import math
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI()

DB_PATH = "litfin.db"

CASE_MULTIPLIERS = {"commercial": 2.5, "personal_injury": 1.8, "ip": 3.0, "employment": 1.5}
NON_CONFORMING_STATES = {"CA", "NV", "UT"}

FAQ = [
    {"id": 1, "question": "What is litigation finance?", "answer": "Third-party funding for legal claims in exchange for a share of proceeds."},
    {"id": 2, "question": "Who can apply?", "answer": "Plaintiffs, law firms, and businesses with pending litigation."},
    {"id": 3, "question": "What happens if we lose?", "answer": "You owe nothing; repayment is typically contingent on success."},
    {"id": 4, "question": "How is ROI calculated?", "answer": "Based on case type multiple, duration, fees, and jurisdiction rules."},
    {"id": 5, "question": "What are the fees?", "answer": "A 2% annual management fee and a 20% success fee on returns."},
    {"id": 6, "question": "Is litigation finance legal everywhere?", "answer": "Mostly, but some states have non-conforming or restrictive rules."},
    {"id": 7, "question": "Does the funder control the case?", "answer": "No, funders cannot make legal or strategic case decisions."},
    {"id": 8, "question": "How long does funding take?", "answer": "Typically 2 to 6 weeks from application to capital deployment."},
    {"id": 9, "question": "What case types are funded?", "answer": "Commercial, personal injury, IP, and employment disputes."},
    {"id": 10, "question": "Can existing settlements be funded?", "answer": "Yes, post-settlement funding is available for delayed payouts."}
]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/case-types")
def get_case_types():
    db = get_db()
    try:
        rows = db.execute("SELECT id, name, return_multiple FROM case_types").fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()

@app.get("/api/jurisdictions")
def get_jurisdictions():
    db = get_db()
    try:
        rows = db.execute("SELECT id, state, is_conforming, notes FROM state_rules").fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()

@app.get("/api/calculator")
def calculate_roi(investment: float = Query(...), case_type: str = Query(...), state: str = Query(...), duration_months: int = Query(...)):
    multiple = CASE_MULTIPLIERS.get(case_type.lower(), 1.0)
    expected_return = investment * multiple
    management_fee = investment * 0.02 * (duration_months / 12)
    success_fee = expected_return * 0.20
    net_return = expected_return - management_fee - success_fee
    if state.upper() in NON_CONFORMING_STATES:
        net_return *= 0.90
    annualized_return = (net_return / investment) ** (12 / duration_months) - 1 if investment > 0 and duration_months > 0 else 0
    return {"investment": investment, "case_type": case_type, "state": state, "duration_months": duration_months, "expected_return": expected_return, "management_fee": management_fee, "success_fee": success_fee, "net_return": net_return, "annualized_return": annualized_return}

@app.get("/api/regulatory-events")
def get_regulatory_events():
    db = get_db()
    try:
        rows = db.execute("SELECT id, state, event_date, description FROM regulatory_events ORDER BY event_date DESC").fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        db.close()

@app.get("/api/faq")
def get_faq():
    return FAQ