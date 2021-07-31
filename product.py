from flask import Flask, json, jsonify, request, Blueprint
from http import HTTPStatus

from db import DB

db = DB()

product_api = Blueprint('product', __name__)

@product_api.route('', methods=['GET'])
def list():
    filter_set = {}
    filter_keys = ('user_id', 'category')
    input_keys = ('lat', 'lng')

    if any(request.args.get(key, type=float) is None for key in input_keys):
        return jsonify(message='Parameter Error', code=HTTPStatus.BAD_REQUEST), HTTPStatus.BAD_REQUEST

    for key in filter_keys:
        if request.args.get(key):
            filter_set[key] = request.args.get(key)

    code, ret = db.list_product(request.args.get('lat', type=float), request.args.get('lng', type=float), filter_set)

    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code

    return jsonify(ret)

@product_api.route('', methods=['POST'])
def create():
    code, ret = db.create_product(request.json)

    if code != HTTPStatus.CREATED:
        return jsonify(message=ret, code=code), code
    
    return ret, code
    

@product_api.route('/<string:uuid>', methods=['GET'])
def retrieve(uuid):
    code, ret = db.retrieve_product(uuid)
    if code != HTTPStatus.OK:
        return jsonify(message=ret), code
    
    return jsonify(ret)

@product_api.route('/<string:uuid>', methods=['PUT'])
def update(uuid):
    code, ret = db.update_product(uuid, request.json)

    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code
    
    return ret, code