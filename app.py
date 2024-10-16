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
    
@app.route('/get_indicators_data', methods=['POST'])
def get_indicators_data():
    client_request = request.get_json()
    date = client_request.get('date')
    
    connection = get_db_connection()

    if connection:
        try:
            indicators = []
         
            tables = ['[dbo].[M_UPIITA]', '[dbo].[M_ESCOM]', '[dbo].[M_CDA]']
            
            for table in tables:
                cursor = connection.cursor()
                
                query = ""
                
                if date not in [None, "", []]:
                    formatted_date = datetime.strptime(date,"%Y-%m-%d")
                    query = f"SELECT ID, Fecha, PM_1, PM2_5, PM_10 FROM {table} WHERE CAST(Fecha AS DATE) = '{formatted_date}'"
                else:
                    query = f"SELECT ID, Fecha, PM_1, PM2_5, PM_10 FROM {table} WHERE CAST(Fecha AS DATE) = (SELECT MAX(CAST(Fecha AS DATE))FROM {table})"
                    
                cursor.execute(query)  
                rows = cursor.fetchall()
                
                if not rows:
                    continue  

                pm1 = 0
                pm2 = 0
                pm10 = 0

                for row in rows:
                    pm1 += row[2]
                    pm2 += row[3]
                    pm10 += row[4]

            
                pm1_average = pm1 / len(rows)
                pm2_average = pm2 / len(rows)
                pm10_average = pm10 / len(rows)
                
                data = {
                    'SOURCE': table,
                    'DATE': row[1],
                    'PM1': pm1_average,
                    'PM25': pm2_average,
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


@app.route('/get_historical_data_upiita', methods=['GET'])
def get_historical_data_upiita():
    connection = get_db_connection()

    if connection:
        try:
            historical_data = []
            
            cursor = connection.cursor()
            
            query = f"SELECT MONTH(Fecha) AS Month, AVG(CAST(PM_1 AS DECIMAL(10, 2))) AS PM_1, AVG(CAST(PM2_5 AS DECIMAL(10, 2))) AS PM2_5, AVG(CAST(PM_10 AS DECIMAL(10, 2))) AS PM_1 FROM [dbo].[M_UPIITA] GROUP BY MONTH(Fecha) ORDER BY Month"
            
            cursor.execute(query)  
            rows = cursor.fetchall()
            
            if not rows:
                return []
            
            for row in rows:
                data = {
                    'MONTH': row[0],
                    'PM1': row[1],
                    'PM25': row[2],
                    'PM10': row[3]
                }
                
                historical_data.append(data)
                    
            return jsonify(historical_data)

        except Exception as ex:
            print(f"Error to execute SQL Query: {ex}")
            return jsonify({'error': f'Error to execute SQL Query: {ex}'})
        
        finally:
            connection.close()
            print("Connection to DB closed.")
    
    else:
        print("Can't connect to DB.")
        return jsonify({'error': 'Can\'t connect to DB.'})
    
@app.route('/get_historical_data_escom', methods=['GET'])
def get_historical_data_escom():
    connection = get_db_connection()

    if connection:
        try:
            historical_data = []
            
            cursor = connection.cursor()
            
            query = f"SELECT MONTH(Fecha) AS Month, AVG(CAST(PM_1 AS DECIMAL(10, 2))) AS PM_1, AVG(CAST(PM2_5 AS DECIMAL(10, 2))) AS PM2_5, AVG(CAST(PM_10 AS DECIMAL(10, 2))) AS PM_1 FROM [dbo].[M_ESCOM] GROUP BY MONTH(Fecha) ORDER BY Month"
            
            cursor.execute(query)  
            rows = cursor.fetchall()
            
            if not rows:
                return []
            
            for row in rows:
                data = {
                    'MONTH': row[0],
                    'PM1': row[1],
                    'PM25': row[2],
                    'PM10': row[3]
                }
                
                historical_data.append(data)
                    
            return jsonify(historical_data)

        except Exception as ex:
            print(f"Error to execute SQL Query: {ex}")
            return jsonify({'error': f'Error to execute SQL Query: {ex}'})
        
        finally:
            connection.close()
            print("Connection to DB closed.")
    
    else:
        print("Can't connect to DB.")
        return jsonify({'error': 'Can\'t connect to DB.'})
    
@app.route('/get_historical_data_cda', methods=['GET'])
def get_historical_data_cda():
    connection = get_db_connection()

    if connection:
        try:
            historical_data = []
            
            cursor = connection.cursor()
            
            query = f"SELECT MONTH(Fecha) AS Month, AVG(CAST(PM_1 AS DECIMAL(10, 2))) AS PM_1, AVG(CAST(PM2_5 AS DECIMAL(10, 2))) AS PM2_5, AVG(CAST(PM_10 AS DECIMAL(10, 2))) AS PM_1 FROM [dbo].[M_CDA] GROUP BY MONTH(Fecha) ORDER BY Month"
            
            cursor.execute(query)  
            rows = cursor.fetchall()
            
            if not rows:
                return []
            
            for row in rows:
                data = {
                    'MONTH': row[0],
                    'PM1': row[1],
                    'PM25': row[2],
                    'PM10': row[3]
                }
                
                historical_data.append(data)
                    
            return jsonify(historical_data)

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
