import sqlite3, os
os.remove('/tmp/litfin_project/litfin.db') if os.path.exists('/tmp/litfin_project/litfin.db') else None
os.makedirs('/tmp/litfin_project', exist_ok=True)
conn = sqlite3.connect('/tmp/litfin_project/litfin.db')
c = conn.cursor()

c.execute('DROP TABLE IF EXISTS case_types')
c.execute('DROP TABLE IF EXISTS jurisdictions')
c.execute('DROP TABLE IF EXISTS fund_performance')
c.execute('DROP TABLE IF EXISTS investor_quals')

c.execute('''CREATE TABLE case_types (
    id INTEGER PRIMARY KEY, name TEXT, typical_duration_months INTEGER,
    expected_return_multiple REAL, risk_score INTEGER)''')
c.execute('''CREATE TABLE jurisdictions (
    id INTEGER PRIMARY KEY, state TEXT, allows_funding BOOLEAN,
    disclosure_required BOOLEAN, registration_required BOOLEAN, notes TEXT)''')
c.execute('''CREATE TABLE fund_performance (
    id INTEGER PRIMARY KEY, fund_name TEXT, vintage_year INTEGER,
    net_irr REAL, multiple REAL, aum_millions REAL)''')
c.execute('''CREATE TABLE investor_quals (
    id INTEGER PRIMARY KEY, type TEXT, min_net_worth REAL,
    min_income REAL, min_investment REAL, description TEXT)''')

case_data = [
    (1,'Medical Malpractice',24,2.5,6),(2,'IP Patent',36,3.0,8),
    (3,'Commercial Contract',18,1.8,4),(4,'Class Action',30,2.0,7),
    (5,'Antitrust',36,2.8,8),(6,'Securities',24,2.2,7),
    (7,'Employment',12,1.5,3),(8,'Environmental',48,3.5,9)]
c.executemany('INSERT INTO case_types VALUES (?,?,?,?,?)', case_data)

juris_data = [
    (1,'NY',1,1,1,'Highly active'),(2,'CA',1,1,1,'Large market'),
    (3,'TX',1,0,0,'Pro-business'),(4,'FL',1,1,0,'Growing'),
    (5,'IL',1,1,1,'Chicago hub'),(6,'DE',1,0,1,'Chancery court'),
    (7,'CT',1,1,1,'Strict rules'),(8,'NV',1,0,0,'Favorable'),
    (9,'PA',1,1,0,'Moderate'),(10,'OH',1,1,0,'Developing')]
c.executemany('INSERT INTO jurisdictions VALUES (?,?,?,?,?,?)', juris_data)

fund_data = [
    (1,'LitFin Alpha',2018,0.15,1.8,150.0),(2,'Justice Capital',2019,0.22,2.1,200.0),
    (3,'LexFund III',2020,0.12,1.5,300.0),(4,'Verity Lit',2017,0.18,1.9,120.0),
    (5,'EquiLaw',2021,0.08,1.3,80.0),(6,'Praesis IV',2016,0.25,2.4,250.0)]
c.executemany('INSERT INTO fund_performance VALUES (?,?,?,?,?,?)', fund_data)

inv_data = [
    (1,'Accredited',1000000,200000,50000,'Standard accredited investor'),
    (2,'Qualified Purchaser',5000000,None,500000,'QP under 40 Act'),
    (3,'Institutional',25000000,None,1000000,'Pension or endowment'),
    (4,'Retail',100000,50000,10000,'Limited participation only')]
c.executemany('INSERT INTO investor_quals VALUES (?,?,?,?,?,?)', inv_data)

conn.commit()
conn.close()
print('OK')
