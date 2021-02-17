#!/usr/bin/env python3
import json
import tarantool

from flask import Flask
from flask import request

TARANTOOL_HOST = "localhost"
TARANTOOL_PORT = 3301
TARANTOOL_PASSWORD = 'pass'

app = Flask(__name__)
server = tarantool.connect(host = TARANTOOL_HOST, port = TARANTOOL_PORT, password = TARANTOOL_PASSWORD)
kv = server.space('kv')

class ResponseError(BaseException):
    def __init__(self, error_code, message, errors = None):
        self.error_code = error_code
        self.message = message
        self.errors = errors

    def format(self):
        if self.errors:
            return {"message" : self.message, "errors" : self.errors}, self.error_code
        else:
            return {"message" : self.message}, self.error_code


def get_body():
    try: 
        j = json.loads(request.json)
    except json.JSONDecodeError as e:
        error_msg = "{} : line {} column {} (char {})".format(e.msg,
                                                              e.lineno,
                                                              e.colno,
                                                              e.pos)
        raise ResponseError(400, "Invalid JSON", error_msg)
    return j


def check_correct_body(j, key_needed = True):
    if not "key" in j and key_needed:
        raise ResponseError(400, "Invalid body", "field key is missing")
    elif not "value" in j:
        raise ResponseError(400, "Invalid body", "field value is missing")

    # field value should be JSON
    elif not isinstance(j["value"], dict):
        raise ResponseError(400, "Invalid body", "field value does not contain JSON")
        

def database_add(key, value):
    try:
        kv.insert((key, json.dumps(value)))
    except:
        raise ResponseError(409, 'key already exists')


def check_key_existance(key):
    if not kv.select(key):
       raise ResponseError(404, 'key doesn\'t exist') 


def database_delete(key):
    kv.delete(key)


def database_get(key):
    # select will return data in the form ((key, value)) and we need value
    return kv.select(key)[0][1]


@app.route("/kv", methods=["POST"])
def post():
    try: 
        body = get_body()
        check_correct_body(body, key_needed = True)
        database_add(body["key"], body["value"])

    except ResponseError as e:
        return e.format()
    
    return '{message : success}', 200    


@app.route("/kv/<key>", methods=["PUT"])
def put(key):
    try: 
        body = get_body()
        check_correct_body(body, key_needed = False)
        database_add(key, body["value"])

    except ResponseError as e:
        return e.format()
    
    return '{message : success}', 200    


@app.route("/kv/<key>", methods=["GET"])
def get(key):
    try:
        check_key_existance(key)
        value = database_get(key)

    except ResponseError as e:
        return e.format()

    return '{value:' + value, 200


@app.route("/kv/<key>", methods=["DELETE"])
def delete(key):
    print(key)
    try:
        check_key_existance(key)
        database_delete(key)

    except ResponseError as e:
        return e.format()

    return '{message : success}', 200    
    



app.run()
