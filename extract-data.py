# coding:utf-8

import sys
from parsers import *
import psycopg2

try:
	conn = psycopg2.connect(dbname="rooms-japan", host="localhost", user="tiphaine", password="tiphsolange")
	cur = conn.cursor()
except:
	print("Unable to connect to database.")
	sys.exit()

# agharta = Agharta("https://www.realestate.co.jp/agharta/ja/rent/listing?page=1&prefecture=JP-13")
agharta = Agharta("https://apartments.gaijinpot.com/ja/rent/listing?prefecture=JP-13&max_price=150000&pets=1")
# agharta.parse() -- should insert directly in db
agharta.load("data.txt")

for p in agharta.props:
	cur.execute("""
		INSERT INTO dwellings (id, rent, admin_fee, surface, walk_time, location)
		VALUES (DEFAULT, %s, %s, %s, %s, %s);""", (p["rent"], p["admin_fee"], p["surface"], p["walk_time"], p["ward"]))
	print(p)
# print(len(agharta.props))

# Insert into database.
conn.commit()
conn.close()