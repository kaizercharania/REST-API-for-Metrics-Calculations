from flask import Flask
import pymysql
from flask_restful import Api
from flask import jsonify

app = Flask(__name__)
api = Api(app)

def connect_sql():
    server = pymysql.connect(
        host='<HOST>',
        user='<USER-NAME>',
        password='<PASSWORD>',
        database='<DATABASE-NAME>')
    return server,server.cursor()

@app.route('/revenue/',methods=['GET'])
def revenue():
    server,cursor = connect_sql()
    try:
        sql = ('''SELECT SUM(sales_amount) , YEAR(transaction_date)  FROM transaction_data 
        GROUP BY YEAR(transaction_date)
        ORDER BY YEAR(transaction_date) DESC;''')
        cursor.execute(sql)
        revenue_dict = {}
        for i in cursor.fetchall():
            revenue_dict[i[1]] = "{0:.2f}".format(i[0])
        revenue = jsonify({'revenue':revenue_dict})
    except pymysql.DatabaseError as error:
        return('error:',error)
    cursor.close()
    return revenue

@app.route('/newusercount/',methods=['GET'])
def newusercount():
    server,cursor = connect_sql()
    try:
        sql = ('''SELECT COUNT(DISTINCT user), YEAR(join_date)  FROM transaction_data
            GROUP BY YEAR(join_date)
            ORDER BY count(user) DESC;''')
        cursor.execute(sql)
        new_user_count_dict = {}
        for i in cursor.fetchall():
            new_user_count_dict[i[1]] = i[0]
        new_user_count = jsonify({'newusercount':new_user_count_dict})
    except pymysql.DatabaseError as error:
        return('error:',error)
    cursor.close()
    return new_user_count

@app.route('/activeusers/',methods=['GET'])
def activeusercount():
    server,cursor = connect_sql()
    try:
        sql = ('''SELECT COUNT(DISTINCT user), YEAR(transaction_date)  FROM transaction_data
            GROUP BY YEAR(transaction_date)
            ORDER BY count(user) DESC;''')
        cursor.execute(sql)
        active_user_count_dict = {}
        for i in cursor.fetchall():
            active_user_count_dict[i[1]] = i[0]
        active_user_count = jsonify({'activeusers':active_user_count_dict})
    except pymysql.DatabaseError as error:
        return('error:',error)
    cursor.close()
    return active_user_count

@app.route('/arpau/',methods=['GET'])
def arpau():
    server,cursor = connect_sql()
    try:
        sql = (''' 
            select T.revenue/T.user_count , T.year from
            (SELECT COUNT(DISTINCT user) as user_count,sum(sales_amount) as revenue, YEAR(transaction_date)  
            as year from transaction_data Group by YEAR(transaction_date))T;''')
        cursor.execute(sql)
        active_user_count_dict = {}
        for i in cursor.fetchall():
            active_user_count_dict[i[1]] = "{0:.2f}".format(i[0])
        active_user_count = jsonify({'AverageRevenuePerActiveUser':active_user_count_dict})
    except pymysql.DatabaseError as error:
        return('error:',error)
    cursor.close()
    return active_user_count

if __name__ == '__main__':
    app.run(use_reloader=True)