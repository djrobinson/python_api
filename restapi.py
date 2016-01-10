import json
import bottle
import time
from datetime import datetime
from bottle import route, run, request, abort
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps

connection = MongoClient("mongodb://localhost:27017")
db = connection.bitfinex
 


start_dt = time.mktime(datetime.strptime("04/01/15 01:00", "%d/%m/%y %H:%M").timetuple()) 
end_dt = time.mktime(datetime.strptime("04/01/15 01:30", "%d/%m/%y %H:%M").timetuple())

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
    entity = list(db['ticks'].aggregate([{"$match": {"date": {"$lt" : end_dt, "$gte" : start_dt  }}}]))


    entity_array = []

    for index in range(len(entity)):
        date = datetime.fromtimestamp(
            entity[index]['date']
        ).strftime('%d/%m/%y %H:%M')
        entity_array.append(date)

        print(entity_array)


    entity = dumps(entity_array)


    if not entity:
        abort(404, 'No document with id %s' % id)
    return entity
 
run(host='localhost', port=8080)


1364767668
1364767709