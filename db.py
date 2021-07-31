from datetime import datetime
from http import HTTPStatus
import http
import uuid
import logging

import pymysql

from config import CONFIG

LIST_PRODUCT = """SELECT * FROM product ORDER BY %s ASC"""
LIST_PRODUCT_FILTER = """SELECT * FROM product WHERE {} ORDER BY %s ASC"""
CREATE_PRODUCT = """INSERT INTO product (uuid, user_id, lat, lng, name, info, number, created_time, category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
UPDATE_PRODUCT = """UPDATE product set {} WHERE uuid = %s"""
RETRIEVE_PRODUCT = """SELECT * FROM product where uuid = %s"""

LIST_ORDER = """SELECT * FROM order ORDER BY %s ASC"""
LIST_ORDER_FILTER = """SELECT * FROM order WHERE {} ORDER BY %s ASC"""


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
            if not data['category'] in ('MEAT', 'VEGETABLE', 'FRUIT', 'BENDONG'):
                return HTTPStatus.BAD_REQUEST, 'Category Error'

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
                if key == 'category' and not data['category'] in ('MEAT', 'VEGETABLE', 'FRUIT', 'BENDONG'):
                    return HTTPStatus.BAD_REQUEST, 'Category Error'
                update_str += self.cursor.mogrify("{}=%s,".format(key), (data[key],))
            
            self.cursor.execute(UPDATE_PRODUCT.format(update_str[:-1]), (uuid,))
            self.commit()

            return HTTPStatus.OK, 'success'
        except Exception as e:
            logging.error('Could not update product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    # def list_product(self, filter_set):
    #     try:
    #         ORDER_FIELD = 'CREATED_TIME'
    #         if len(filter_set) == 0:
    #             self.cursor.execute(LIST_PRODUCT, (ORDER_FIELD,))
    #         else:
    #             filter_str = ''
    #             for f in filter_set:
    #                 filter_str += self.cursor.mogrify("{}=%s".format(f), (filter_set[f],))
    #             self.cursor.execute(LIST_PRODUCT_FILTER.format(filter_str), (ORDER_FIELD,))
            
    #         return HTTPStatus.OK, self.cursor.fetchall()
    #     except Exception as e:
    #         logging.error('Could not list product (reason: %r)', e)
    #         return HTTPStatus.BAD_REQUEST, 'Something Error Happened'