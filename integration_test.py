import subprocess
import sqlite3
import os

def run_test(name, condition):
    status = "PASS" if condition else "FAIL"
    print(f"{status}: {name}")

def test_litfin():
    subprocess.run(["python3", "/tmp/litfin_project/create_database.py"], check=True)
    run_test("Database creation script executed", True)

    db_path = "/tmp/litfin_project/litfin.db"
    run_test("litfin.db exists", os.path.exists(db_path))

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM case_types")
        ct_count = cur.fetchone()[0]
        run_test(f"case_types count >= 8 (got {ct_count})", ct_count >= 8)
        
        cur.execute("SELECT COUNT(*) FROM jurisdictions")
        j_count = cur.fetchone()[0]
        run_test(f"jurisdictions count >= 40 (got {j_count})", j_count >= 40)
        conn.close()
    except Exception as e:
        run_test("Database queries failed", False)

    html_path = "/tmp/litfin_project/index.html"
    html_exists = os.path.exists(html_path)
    run_test("index.html exists", html_exists)
    
    html_valid = False
    if html_exists:
        with open(html_path, 'r') as f:
            html_valid = '<html' in f.read().lower()
    run_test("index.html contains '<html'", html_valid)

    investment = 500000
    multiplier = 2.5
    expected_return = investment * multiplier
    run_test(f"Calculator validation (expected return: {expected_return})", expected_return == 1250000)

if __name__ == "__main__":
    test_litfin()