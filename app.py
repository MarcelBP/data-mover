#!/usr/bin/env python

import decimal
import json
import logging
import datetime
import os
import sys
import yaml
import pandas as pd
from sqlalchemy import create_engine
from flask import Flask, Response, abort, request, json, jsonify
from functools import wraps
from urlparse import urlparse, urlunparse

app = Flask(__name__)

def path_to_dict(path):
    d = {'name': os.path.basename(path)}
    if os.path.isdir(path):
        d['type'] = "directory"
        d['children'] = [path_to_dict(os.path.join(path,x)) for x in os.listdir(path)]
    else:
        d['type'] = "file"
    return d

def get_db_creds(database):
	return pd.read_json('conf.d/' + database + '.json')

def connect_db(creds):
	return create_engine(creds['engine'][0] + '://' + cred['user'][0] + ':' + 
		creds['password'][0] + '@' + creds['hostname'][0] + '/' + creds['db'][0])

@app.route("/list", methods=['GET'])
def return_database_list():
	return json.dumps(path_to_dict('conf.d/'))

@app.route("/connect/<string:database>", methods=['POST'])
# Any of the supported SQLAlchemy engines/dialects """
# http://docs.sqlalchemy.org/en/latest/dialects/ """

def add_database_json(database=None):
	print request.data
	open('conf.d/' + request.args.get('database') +
		 '.json', 'r+').write(str(jsonify(request.get_json(force=True))))
	return "Success"

@app.route("/query/<database>", methods=['POST', 'GET'])
def do_query(database=None):

    creds = get_db_creds(database)

    if not creds:
        abort(404)

    engine = connect_db(creds)

    sql = request.form.get('sql')
    if not sql:
        sql = request.args.get('sql')
        if not sql:
        	abort(404)

    if '%' in sql:
        sql = sql.replace('%', '%%')

    # Attempt to run the query
    try:
        results = pd.read_sql(engine, sql)
    except Exception, e:
        return {"ERROR": ": ".join(str(i) for i in e.args)}
    
    # return anything pandas supports (i.e. msgpack, stata, hdf, json, etc.)#  
    return results.to_json()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
