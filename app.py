import os
import pyodbc
from flask import Flask, redirect, render_template, request, url_for, jsonify
from datetime import datetime

app = Flask(__name__)


def get_db_connection():
    try:
        print("Trying to connect to database...")
        connection = pyodbc.connect('DRIVER={SQL Server};SERVER=CARLOSEMILIO;DATABASE=Mediciones;UID=sa;PWD=root')
        print("Connection succeded")
        return connection
    except Exception as ex:
        print(f"Error to reach connection: {ex}")
        return None



@app.route('/get_data', methods=['POST'])
def get_data():
    client_request = request.get_json()
    date = client_request.get('date')

    connection = get_db_connection()

    if connection:
        try:
            print(client_request)
            cursor = connection.cursor()
            query = ""
            if date not in [None, "", []]:
                formatted_date = datetime.strptime(date,"%Y-%m-%d")
                query = f"SELECT ID, PM_1, PM2_5, PM_10 FROM M_UPIITA WHERE Fecha = CONVERT(datetime,'{date}')" 
            else:
                query = "SELECT ID, Fecha, PM_1, PM2_5, PM_10 FROM M_UPIITA WHERE CAST(Fecha AS DATE) = (SELECT MAX(CAST(Fecha AS DATE))FROM M_UPIITA)"
            cursor.execute(query)
            rows = cursor.fetchall()
            
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
                'DATE' : row[1],
                'PM1': pm1_average,
                'PM2.5': pm2_average,
                'PM10': pm10_average
            }

            return data

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
