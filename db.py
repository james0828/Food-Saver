from datetime import datetime
from http import HTTPStatus
import http
import uuid
import logging

import pymysql

from config import CONFIG

LIST_PRODUCT = """SELECT *, 6378.138 * 2 * ASIN(
      SQRT(
        POW(
          SIN(
            (
              %s * PI() / 180 - lat * PI() / 180
            ) / 2
          ), 2
        ) + COS(%s * PI() / 180) * COS(lat * PI() / 180) * POW(
          SIN(
            (
              %s * PI() / 180 - lng * PI() / 180
            ) / 2
          ), 2
        )
      )
    ) *1000 AS distance FROM product ORDER BY distance ASC"""
LIST_PRODUCT_FILTER = """SELECT *, 6378.138 * 2 * ASIN(
      SQRT(
        POW(
          SIN(
            (
              %s * PI() / 180 - lat * PI() / 180
            ) / 2
          ), 2
        ) + COS(%s * PI() / 180) * COS(lat * PI() / 180) * POW(
          SIN(
            (
              %s * PI() / 180 - lng * PI() / 180
            ) / 2
          ), 2
        )
      )
    ) *1000 AS distance FROM product WHERE {} ORDER BY distance ASC"""
CREATE_PRODUCT = """INSERT INTO product (uuid, user_id, lat, lng, name, info, number, created_time, category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
UPDATE_PRODUCT = """UPDATE product set {} WHERE uuid = %s"""
RETRIEVE_PRODUCT = """SELECT * FROM product where uuid = %s"""

LIST_ORDER = """SELECT * FROM orders ORDER BY stars DESC, created_time ASC"""
LIST_ORDER_FILTER = """SELECT * FROM orders WHERE {} ORDER BY stars DESC, created_time ASC"""
CREATE_ORDER = """INSERT INTO orders (uuid, owner_id, buyer_id, created_time, product_id)
    VALUES (%s, %s, %s, %s, %s)
"""
RETRIEVE_ORDER = """SELECT * FROM orders where uuid = %s"""
UPDATE_ORDER = """UPDATE orders set {} WHERE uuid = %s"""

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
    
    def list_product(self, lat, lng, filter_set):
        try:
            if len(filter_set) == 0:
                self.cursor.execute(LIST_PRODUCT, (lat, lat, lng))
            else:
                filter_str = ''
                for f in filter_set:
                    if filter_str != '':
                        filter_str += self.cursor.mogrify(" AND {}=%s".format(f), (filter_set[f],))
                    else:
                        filter_str += self.cursor.mogrify("{}=%s".format(f), (filter_set[f],))

                self.cursor.execute(LIST_PRODUCT_FILTER.format(filter_str), (lat, lat, lng))
            
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
    
    def list_order(self, filter_set):
        try:
            if len(filter_set) == 0:
                self.cursor.execute(LIST_ORDER)
            else:
                filter_str = ''
                for f in filter_set:
                    if filter_str != '':
                        filter_str += self.cursor.mogrify(" AND {}=%s".format(f), (filter_set[f],))
                    else:
                        filter_str += self.cursor.mogrify("{}=%s".format(f), (filter_set[f],))

                self.cursor.execute(LIST_ORDER_FILTER.format(filter_str))
            
            return HTTPStatus.OK, self.cursor.fetchall()
        except Exception as e:
            logging.error('Could not list order (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    def create_order(self, data):
        try:
            self.cursor.execute(CREATE_ORDER, (uuid.uuid4(), data['owner_id'], data['buyer_id'], datetime.now(), data['product_id']))
            self.commit()

            return HTTPStatus.CREATED, 'Success'
        except Exception as e:
            logging.error('Could not create order (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    def retrieve_order(self, uuid):
        try:
            self.cursor.execute(RETRIEVE_ORDER, (uuid, ))
            ret = self.cursor.fetchone()
            if ret is None:
                return HTTPStatus.NOT_FOUND, 'Not Found'
            
            return HTTPStatus.OK, ret
        except Exception as e:
            logging.error('Could not retrieve order (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'
    
    def update_order(self, uuid, data):
        try:
            update_str = ''
            for key in data:
                update_str += self.cursor.mogrify("{}=%s,".format(key), (data[key],))
            
            self.cursor.execute(UPDATE_ORDER.format(update_str[:-1]), (uuid,))
            self.commit()

            return HTTPStatus.OK, 'success'
        except Exception as e:
            logging.error('Could not update product (reason: %r)', e)
            return HTTPStatus.BAD_REQUEST, 'Something Error Happened'