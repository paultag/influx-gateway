import sys
import datetime as dt

from influxdb import InfluxDBClient
from flask import Flask, request

import configparser

config = configparser.ConfigParser()
config.read('conf.ini')

app = Flask(__name__)

def get_client(conf):
    host, user, password, database = [conf[x] for x in [
        'host', 'user', 'password', 'database',
    ]]
    return InfluxDBClient(host, 8086, user, password, database)


@app.route("/submit/<table>", methods=['POST'])
def submit(table):
    if table not in config:
        raise Exception("Unknown config")

    conf = config[table]
    client = get_client(conf)
    _indexable_fields = conf['index_on'].split(",")

    fields = request.get_json()
    tags = {x: fields[x] for x in _indexable_fields if x in fields}
    when = int(fields[conf['timestamp']]) * int(conf['timestamp_multiplier'])

    data = {
        "measurement": table,
        "tags": tags,
        "time": when,
        "fields": fields,
    }

    client.write_points([data])
    return ""


if __name__ == "__main__":
    app.run(debug=True)
