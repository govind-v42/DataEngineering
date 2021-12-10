from pymongo import MongoClient
import pandas as pd
import json
import certifi

ca = certifi.where()

cluster = MongoClient("mongodb+srv://dbUser:admin123@cluster0.wep5o.mongodb.net/test", tlsCAFile=ca)

db = cluster['NYT']

collection = db['collection']



