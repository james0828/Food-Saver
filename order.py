from flask import Flask, jsonify, request, Blueprint
from http import HTTPStatus
from distutils import util

from db import DB

order_api = Blueprint('order', __name__)

db = DB()

@order_api.route('', methods=['GET'])
def list():
    filter_set = {}
    filter_keys = ('status', 'owner_id', 'buyer_id')

    for key in filter_keys:
        if request.args.get(key):
            if key == 'status':
                try:
                    filter_set[key] = util.strtobool(request.args.get(key))
                except:
                    return jsonify(message='Truth Value Error', code=HTTPStatus.BAD_REQUEST), HTTPStatus.BAD_REQUEST
            else:
                filter_set[key] = request.args.get(key)

    code, ret = db.list_order(filter_set)

    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code

    return jsonify(ret)

@order_api.route('', methods=['POST'])
def create():
    code, ret = db.create_order(request.json)

    if code != HTTPStatus.CREATED:
        return jsonify(message=ret, code=code), code
    
    return ret, code

@order_api.route('/<string:uuid>', methods=['GET'])
def retrieve(uuid):
    code, ret = db.retrieve_order(uuid)
    if code != HTTPStatus.OK:
        return jsonify(message=ret), code
    
    return jsonify(ret)

@order_api.route('/<string:uuid>', methods=['PUT'])
def update(uuid):
    code, ret = db.update_order(uuid, request.json)

    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code
    
    return ret, code

@order_api.route('/<string:uuid>', methods=['POST'])
def complete(uuid):
    code, ret = db.complete_order(uuid)

    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code
    
    return ret, code
