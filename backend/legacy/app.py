from flask import Flask, jsonify
import mysql.connector


app = Flask(__name__)

def write_to_customer_data(firstName: str, lastName: str):
    config = {
        'user': 'root',
        'password': 'password',
        'host': 'db',
        'port': '3306',
        'database': 'production'
    }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    query = "INSERT INTO Customers (FirstName, LastName) VALUES (%s, %s)"
    values = (firstName, lastName)
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    connection.close()

def get_customer_data():
    config = {
        'user': 'root',
        'password': 'password',
        'host': 'db',
        'port': '3306',
        'database': 'production'
    }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute('SELECT * FROM Customers')
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results


@app.route('/')
def index():
    return jsonify({'Customer Data': get_customer_data()})

@app.route('/addcustomer')
def index():
    return jsonify({'Customer Data': write_to_customer_data()})

if __name__ == '__main__':
    app.run(host='0.0.0.0')