import json
import bottle
import time
from datetime import datetime
from bottle import route, run, request, abort
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
import dateutil.parser as parser

connection = MongoClient("mongodb://localhost:27017")
db = connection.bitfinex
 


start_dt = datetime.strptime("16/01/70 06:00", "%d/%m/%y %H:%M")
end_dt = datetime.strptime("16/01/70 20:00", "%d/%m/%y %H:%M")

# start_dt = datetime.fromtimestamp(start_dt)
# end_dt = datetime.fromtimestamp(end_dt)

# start_dt = 1364767668
# end_dt = 1364767709

print(start_dt, end_dt)

@route('/ticks', method='PUT')
def put_tick():
    data = request.body.readline()
    if not data:
        abort(400, 'No data received')
    entity = json.loads(data)
    if not entity.has_key('_id'):
        abort(400, 'No _id specified')
    try:
        db['documents'].save(entity)
    except ValidationError as ve:

        abort(400, str(ve))
     
@route('/ticks', method='GET')
def get_ticks():

    entity = list(db['ticks'].aggregate([{"$match": {"date": {"$lt" : end_dt, "$gte" : start_dt }}},
     {"$project": {
         "year":       {"$year": "$date"},
         "month":      {"$month": "$date"},
         "day":        {"$dayOfMonth": "$date"},
         "hour":       {"$hour": "$date"},
         "minute":     {"$minute": "$date"},
         "second":     {"$second": "$date"},
         "date": 1,
         "price": 1 }},
     {"$sort": {"date": 1}},
     {"$group":
          {"_id" : {"year": "$year", "month": "$month", "day": "$day", "hour": "$hour", "minute": "$minute" },
                    "open":  {"$first": "$price"},
                    "high":  {"$max": "$price"},
                    "low":   {"$min": "$price"},
                    "close": {"$last": "$price"} }}
                    ]))

    print(entity)

    entity = dumps(entity)


    if not entity:
        abort(404, 'No document with id %s' % id)
    return entity
 
run(host='localhost', port=8080)

