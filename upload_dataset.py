'''

Engenharia de Dados - Exportação de Dados Para o SQLite
Felipe Daniel Dias dos Santos - 11711ECP004
Graduação em Engenharia de Computação - Faculdade de Engenharia Elétrica - Universidade Federal de Uberlândia

'''

import sqlite3
import pandas as pd

con = sqlite3.connect("challenge_database.db")
cur = con.cursor()

df = pd.read_excel("challenge_dataset.xlsx")
df.to_sql(name = "road_trip", con = con, if_exists = "append")