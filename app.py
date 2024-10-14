import os
import pyodbc
from flask import Flask, redirect, render_template, request, url_for, jsonify
from datetime import datetime
from dotenv import load_dotenv

app = Flask(__name__)


def get_db_connection():
    try:
        print("Trying to connect to database...")
        load_dotenv()
        database = os.environ.get('AZURE_SQL_DATABASE')
        server = os.environ.get('AZURE_SQL_SERVER')
        uid = os.environ.get('AZURE_SQL_USER')
        pwd = os.environ.get('AZURE_SQL_PASSWORD')
        print(database,server,uid,pwd)
        
        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}')
        print("Connection succeded")
        return connection
    except Exception as ex:
        print(f"Error to reach connection: {ex}")
        return None



@app.route('/get_indicators_data', methods=['GET'])
def get_indicators_data():

    connection = get_db_connection()

    if connection:
        try:
            indicators = []
            for table in ['[dbo].[M_UPIITA]']:
                cursor = connection.cursor()
                query = f"SELECT ID, Fecha, PM_1, PM2_5, PM_10 FROM {table}"
                cursor.execute(query)
                rows = cursor.fetchall()
                
                if not rows:
                    return []

                pm1 = 0
                pm2 = 0
                pm10 = 0

                for row in rows:
                    pm1 = pm1 + row[2]
                    pm2 = pm2 + row[3]
                    pm10 = pm10 + row[4]

                pm1_average = pm1/len(rows)
                pm2_average = pm2/len(rows)
                pm10_average = pm10/len(rows)
                
                data = {
                    'SOURCE': table,
                    'DATE' : row[1],
                    'PM1': pm1_average,
                    'PM2.5': pm2_average,
                    'PM10': pm10_average
                }
                
                indicators.append(data)
            return jsonify(indicators)

        except Exception as ex:
            print(f"Error to execute SQL Query: {ex}")
            return jsonify({'error': f'Error to execute SQL Query: {ex}'})
        
        finally:
            connection.close()
            print("Connection to DB closed.")
    
    else:
        print("Can't connect to DB.")
        return jsonify({'error': 'Can\'t connect to DB.'})

if __name__ == '__main__':
    print("Servidor Flask ...")
    app.run(debug=True)
