from re import M
from flask import Flask, render_template, request
import datetime
import json

app = Flask(__name__, static_url_path='', static_folder='static')

pulse_count = 60000

import mysql.connector

def connect_to_mysql():
    mydb = mysql.connector.connect(
        host="miilliionpagesdb-1.cozrhtxho5bm.ap-south-1.rds.amazonaws.com",
        user="miilliionpages",
        password="shreevallabha",
        database="saienterprises"
    )
    mycursor = mydb.cursor()

    return mydb, mycursor

def close_connection(mydb, mycursor):
    mycursor.close()
    mydb.close()

def get_integer_value_from_raw_data(string_data):
    data_array = string_data.split("/")
    multiple = int(data_array[0])
    adding_portion = int(data_array[1])
    return (multiple*pulse_count) + adding_portion


def push_data_to_database(project_id, gateway_id, datetimestamp, transmitter_id, sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6):
    sensor_1, sensor_2 = get_integer_value_from_raw_data(sensor_1), get_integer_value_from_raw_data(sensor_2)
    sensor_3, sensor_4 = get_integer_value_from_raw_data(sensor_3), get_integer_value_from_raw_data(sensor_4)
    sensor_5, sensor_6 = get_integer_value_from_raw_data(sensor_5), get_integer_value_from_raw_data(sensor_6)
    sql = "INSERT INTO sensor_data (project_id, gateway_id, datetime, transmitter_id, sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (project_id, gateway_id, datetimestamp, transmitter_id, sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6)
    mydb, mycursor = connect_to_mysql()
    mycursor.execute(sql, val)
    mydb.commit()
    close_connection(mydb, mycursor)

@app.route('/')
def root():
   return render_template('pages/dashboard.html')

@app.route('/api-fetch-data')
def api_fetch_data():
    mydb, mycursor = connect_to_mysql()
    sql = "SELECT JSON_ARRAYAGG(json_object('project_id', project_id, 'gateway_id', gateway_id,'datetime', datetime, 'transmitter_id', transmitter_id, 'sensor_1', sensor_1,'sensor_2', sensor_2, 'sensor_3', sensor_3, 'sensor_4', sensor_4,'sensor_5', sensor_5, 'sensor_6', sensor_6)) FROM sensor_data"
    mycursor.execute(sql)
    responseJson = mycursor.fetchone()
    print(responseJson[0])
    close_connection(mydb, mycursor)
    response = {}
    response["data"] = json.loads(responseJson[0])
    return response


@app.route('/push_to_iot',methods = ['POST', 'GET'])
def push_to_iot():
    if request.method == 'POST':
       project_id = str(request.form['project_id'])
       gateway_id = int(request.form['gateway_id'])
       datetimestamp = datetime.datetime.now()
       transmitter_id = str(request.form['transmitter_id'])
       sensor_1 = str(request.form['sensor_1'])
       sensor_2 = str(request.form['sensor_2'])
       sensor_3 = str(request.form['sensor_3'])
       sensor_4 = str(request.form['sensor_4'])
       sensor_5 = str(request.form['sensor_5'])
       sensor_6 = str(request.form['sensor_6'])
       push_data_to_database(project_id, gateway_id, datetimestamp, transmitter_id, sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6)

    if request.method == 'GET':
        project_id = str(request.args.get('project_id'))
        gateway_id = int(request.args.get('gateway_id'))
        datetimestamp = datetime.datetime.now()
        transmitter_id = str(request.args.get('transmitter_id'))
        sensor_1 = str(request.args.get('sensor_1'))
        sensor_2 = str(request.args.get('sensor_2'))
        sensor_3 = str(request.args.get('sensor_3'))
        sensor_4 = str(request.args.get('sensor_4'))
        sensor_5 = str(request.args.get('sensor_5'))
        sensor_6 = str(request.args.get('sensor_6'))
        push_data_to_database(project_id, gateway_id, datetimestamp, transmitter_id, sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6)
    
    return {"status":200, "message":"successfully pushed the data to cloud"}
      


if __name__ == '__main__':
	app.run(debug=True)
