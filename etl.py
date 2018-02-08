#!/usr/bin/python

#####################################################################
# This script load Flight data in batch into MongoDB
# Data are load from all the CSV file present in the current directory
# and once processed, moved to a processed folder
#####################################################################

import csv, os, glob, calendar, copy, decimal
from datetime import datetime, date, time
#import calendar, copy, decimal
from pymongo import MongoClient
from collections import defaultdict


client = MongoClient("localhost:27017")
db = client.competition
flights = db.flights

bulk = flights.initialize_ordered_bulk_op()
bulk_count = 0
if not os.path.exists("./processed"):
    os.makedirs("./processed")
os.chdir(".")
for name in glob.glob("*.csv"):
    print(name)
    with open(name, 'r') as csvfile:
        spamreader = csv.DictReader(csvfile)
        spamreader.__next__()
        for row in spamreader:
           #print((row))
           doc = { "_id" : row["id"],
                "observation_date" : datetime.strptime(row["observation_date"],"%Y-%m-%d"),
                "observation_time" : row["observation_time"],
                "pos" : row["pos"],
                "origin" : row["origin"],
                "destination" : row["destination"],
                "is_one_way" : (True if row["is_one_way"]==1 else False),
                "outbound_travel_stop_over" : row["outbound_travel_stop_over"].split(','),
                "inbound_travel_stop_over" : row["inbound_travel_stop_over"].split(','),
                "carrier" : row["carrier"],
                "outbound_flight_no" : row["outbound_flight_no"].split(','),
                "inbound_flight_no" : row["inbound_flight_no"].split(','),
                "outbound_departure_date" : (datetime.strptime(row["outbound_departure_date"],"%Y-%m-%d") if row["outbound_departure_date"]!="" else ""),
                "outbound_departure_time" : row["outbound_departure_time"],
                "outbound_arrival_date" : (datetime.strptime(row["outbound_arrival_date"],"%Y-%m-%d") if row["outbound_arrival_date"]!="" else ""),
                "outbound_arrival_time" : row["outbound_arrival_time"],
                "inbound_departure_date" : (datetime.strptime(row["inbound_departure_date"],"%Y-%m-%d") if row["inbound_departure_date"]!="" else ""),
                "inbound_departure_time" : row["inbound_departure_time"],
                "inbound_arrival_date" : (datetime.strptime(row["inbound_arrival_date"],"%Y-%m-%d") if row["inbound_arrival_date"]!="" else ""),
                "inbound_arrival_time" : row["inbound_arrival_time"],
                "outbound_fare_basis" : row["outbound_fare_basis"].split(','),
                "inbound_fare_basis" : row["inbound_fare_basis"].split(','),
                "outbound_booking_class" : row["outbound_booking_class"].split(','),
                "inbound_booking_class" : row["inbound_booking_class"].split(','),
                "price_exc" : (float(row["price_exc"]) if row["price_exc"]!="" else float(0)),
                "price_inc" : (float(row["price_inc"]) if row["price_inc"]!="" else float(0)),
                "tax" : (float(row["tax"]) if row["tax"]!="" else float(0)),
                "currency" : row["currency"],
                "source" : row["source"],
                "price_outbound" : (float(row["price_outbound"]) if row["price_outbound"]!="" else float(0)),
                "price_inbound" : (float(row["price_inbound"]) if row["price_inbound"]!="" else float(0)),
                "is_tax_inc_outin" : (True if row["is_tax_inc_outin"]==1 else False),
                "search_class" : row["search_class"],
                "outbound_fare_family" : row["outbound_fare_family"],
                "inbound_fare_family" : row["inbound_fare_family"],
                "outbound_seats" : row["outbound_seats"].split(','),
                "inbound_seats" :  row["inbound_seats"],
                "min_stay" : (int(row["min_stay"]) if row["min_stay"]!="" else 0),
                "outbound_flight_duration" : (int(row["outbound_flight_duration"]) if row["outbound_flight_duration"]!="" else 0),
                "inbound_flight_duration" : (int(row["inbound_flight_duration"]) if row["inbound_flight_duration"]!="" else 0)}
           if bulk_count > 0 and bulk_count % 1000 == 0:
              bulk.execute()
              bulk = flights.initialize_ordered_bulk_op()
              bulk_count = 0
           bulk.insert(doc)
           bulk_count += 1
    # Close and move the file to the processes folder
    os.rename(name, "./processed/"+name)

if bulk_count > 0:
   bulk.execute()
client.close()
