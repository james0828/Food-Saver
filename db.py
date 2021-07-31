from datetime import datetime
from http import HTTPStatus
import http
import uuid
import logging

import pymysql

from config import CONFIG

# TOTAL_TEAM = """SELECT COUNT(*) as c FROM team"""
# TOTAL_TEAM_STATUS = """SELECT COUNT(*) as c FROM team WHERE STATUS=%s"""
# LIST_TEAM = """SELECT * FROM team ORDER BY CREATED_TIME ASC LIMIT %s OFFSET %s"""
# LIST_TEAM_WITH_STATUS = """SELECT * FROM team WHERE STATUS=%s ORDER BY CREATED_TIME ASC LIMIT %s OFFSET %s"""
# RETRIEVE_TEAM = """SELECT * FROM team where uuid = %s"""
# CREATE_TEAM = """INSERT INTO team (uuid, school_name, school_grade, first_leader_name, first_leader_mail, first_leader_phone,
#     second_leader_name, second_leader_mail, second_leader_phone, attachment_url, third_user_name, fourth_user_name,
#     fifth_user_name, created_time, under_eighteen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
# UPDATE_TEAM = """UPDATE team set status = %s, modified_time = %s where uuid = %s"""

LIST_PRODUCT = """SELECT * FROM product ORDER BY %s ASC"""
LIST_PRODUCT_FILTER = """SELECT * FROM product WHERE {} ORDER BY %s ASC"""
CREATE_PRODUCT = """INSERT INTO product (uuid, user_id, lat, lng, name, info, number, created_time, category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
UPDATE_PRODUCT = """UPDATE product set {} WHERE uuid = %s"""
RETRIEVE_PRODUCT = """SELECT * FROM product where uuid = %s"""


class DB:
    def __init__(self):
        self.connect()

    def connect(self):
        self.connection = pymysql.connect(
            host=CONFIG['DB_HOST'],
            port=CONFIG['DB_PORT'],
            user=CONFIG['DB_USER'],
            password=CONFIG['DB_PASS'],
            database=CONFIG['DB_NAME'],
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()
    
    def list_product(self, filter_set):
        try:
            ORDER_FIELD = 'CREATED_TIME'
            if len(filter_set) == 0:
                self.cursor.execute(LIST_PRODUCT, (ORDER_FIELD,))
            else:
                filter_str = ''
                for f in filter_set:
                    filter_str += self.cursor.mogrify("{}=%s".format(f), (filter_set[f],))
                self.cursor.execute(LIST_PRODUCT_FILTER.format(filter_str), (ORDER_FIELD,))
            
            return HTTPStatus.OK, self.cursor.fetchall()
        except Exception as e:
            logging.error('Could not list product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    def create_product(self, data):
        try:
            self.cursor.execute(CREATE_PRODUCT, (str(uuid.uuid4()), data['user_id'], data['lat'], data['lng'],
                data['name'], data['info'], data['number'], datetime.now(), data['category']
            ))
            self.commit()

            return HTTPStatus.CREATED, 'Success'
        except Exception as e:
            logging.error('Could not create product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'

    def retrieve_product(self, uuid):
        try:
            self.cursor.execute(RETRIEVE_PRODUCT, (uuid, ))
            ret = self.cursor.fetchone()
            if ret is None:
                return HTTPStatus.NOT_FOUND, 'Not Found'
            
            return HTTPStatus.OK, ret
        except Exception as e:
            logging.error('Could not retrieve product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    def update_product(self, uuid, data):
        try:
            update_str = ''
            for key in data:
                print(key, data[key])
                update_str += self.cursor.mogrify("{}=%s,".format(key), (data[key],))
            
            self.cursor.execute(UPDATE_PRODUCT.format(update_str[:-1]), (uuid,))
            self.commit()

            return HTTPStatus.OK, 'success'
        except Exception as e:
            logging.error('Could not update product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'