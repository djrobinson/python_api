import json
import bottle
from bottle import route, run, request, abort
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps

connection = MongoClient("mongodb://localhost:27017")
db = connection.bitfinex
 

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
    entity = dumps(db['ticks'].find_one())


    if not entity:
        abort(404, 'No document with id %s' % id)
    return entity
 
run(host='localhost', port=8080)

