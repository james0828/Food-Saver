from flask import Flask, json, jsonify, request, Blueprint
from http import HTTPStatus
from distutils import util

from db import DB

user_location_api = Blueprint('user_location', __name__)

db = DB()

@user_location_api.route('', methods=['GET'])
def list():
    code, ret = db.list_user_location(request.args.get('user_id'))
    
    if code != HTTPStatus.OK:
        return jsonify(message=ret, code=code), code
    
    return jsonify(ret), code

@user_location_api.route('', methods=['POST'])
def create():
    args = ('user_id', 'lat', 'lng')
    if any(request.json.get(key) is None for key in args):
        return jsonify(message='Parameter Error', code=HTTPStatus.BAD_REQUEST), HTTPStatus.BAD_REQUEST

    code, ret = db.create_user_location(request.json)
    
    if code != HTTPStatus.CREATED:
        return jsonify(message=ret, code=code), code
    
    return ret, code

@user_location_api.route('/<string:uuid>', methods=['DELETE'])
def delete(uuid):
    code, ret = db.delete_user_location(uuid)

    if code != HTTPStatus.NO_CONTENT:
        return jsonify(message=ret, code=code), code
    
    return ret, code
