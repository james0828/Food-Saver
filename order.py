from flask import Flask, jsonify, request, Blueprint

from db import DB

order_api = Blueprint('order', __name__)

db = DB()

@order_api.route('', methods=['GET'])
def list():
    return 'LIST'

@order_api.route('', methods=['POST'])
def create():
    return 'CREATE'

@order_api.route('/<string:uuid>', methods=['GET'])
def retrieve():
    return 'RETRIEVE'

@order_api.route('/<string:uuid>', methods=['PUT'])
def update():
    return 'PUT'