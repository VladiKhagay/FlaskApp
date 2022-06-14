from flask import Flask, request, jsonify
import psycopg2
from MySqlManager import MySqlManager
from MongoDBManager import MongoDBManager
from PostgreSqlManager import PostgreSqlManager
import os


app = Flask(__name__)

MYSQL_DB_URL = os.environ['MYSQL_DB_URL']
MYSQL_DB_USER = os.environ['MYSQL_DB_USER']
MYSQL_DB_PASSWORD = os.environ['MYSQL_DB_PASSWORD']
MYSQL_DB_DBNAME = os.environ['MYSQL_DB_DBNAME']

PG_DB_URL = os.environ['PG_DB_URL']
PG_DB_USER = os.environ['PG_DB_USER']
PG_DB_PASSWORD = os.environ['PG_DB_PASSWORD']
PG_DB_DBNAME = os.environ['PG_DB_DBNAME']


mysql_db = MySqlManager(host=MYSQL_DB_URL, user=MYSQL_DB_USER,
                        password=MYSQL_DB_PASSWORD, database=MYSQL_DB_DBNAME)
pg_db = PostgreSqlManager(host=PG_DB_URL, database=PG_DB_DBNAME,
                          user=PG_DB_USER, password=PG_DB_USER)
mongo_db = MongoDBManager()


@app.route('/')
def index():
    cursor = mysql_db.cursor()
    cursor.execute("SELECT * FROM ACTIVITY")
    res = cursor.fetchall()
    return jsonify(res)


@app.route('/iob/instances', methods=['POST'])
def create_instance():
    if request.method == 'POST':
        if request.get_json() is not None:
            return jsonify(pg_db.insert_instance(request.get_json()))


@app.route('/iob/instances/<instanceDomain>/<instanceId>', methods=['PUT'])
def update_instance(instanceDomain, instanceId):
    if request.method == 'PUT':
        if request.args.get("userDomain") is not None and \
                request.args.get("userEmail") is not None:
            if request.get_json() is not None:
                result = pg_db.update_instance(request.get_json())
                return jsonify(result)


@app.route('/iob/instances/search/byType/<type>')
def get_instances_by_type(type):
    if request.method == 'GET':
        if request.view_args['type'] == 'Wishlist':
            if request.args.get("userDomain") is not None and \
                    request.args.get("userEmail") is not None and \
                    request.args.get("size") is not None and \
                    request.args.get("page") is not None:
                result = pg_db.select_instances_by_type('Wishlist', request.args.get("userDomain"),
                                                        request.args.get("userEmail"))
                return jsonify(result)
        elif request.view_args['type'] == 'Shop':
            result = mongo_db.get_collection_items()
            return jsonify(result)


@app.route('/iob/activities', methods=['POST'])
def invoke_activity():
    if request.method == 'POST':
        if request.get_json() is not None:
            result = mysql_db.insert_activity(request.get_json())
            return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
