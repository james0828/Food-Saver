from http import HTTPStatus

from flask import Flask, jsonify, request, Blueprint
from flask_cors import CORS, cross_origin

from product import product_api
from order import order_api
from user_location import user_location_api

app = Flask(__name__)
cors = CORS(app)

app.register_blueprint(order_api, url_prefix='/api/order')
app.register_blueprint(product_api, url_prefix='/api/product')
app.register_blueprint(user_location_api, url_prefix='/api/user_location')