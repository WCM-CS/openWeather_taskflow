import pymysql 

try:
    connection = pymysql.connect(
        host='127.0.0.1',  # or 'localhost'
        user='root',
        password='password',
        database='WeatherData',
        port=3306
    )
    print("Connection successful!")
except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL: {e}")
finally:
    if connection:
        connection.close()