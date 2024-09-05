# CRUD TIPO_PLANTILLA
# Get one, Get All, Post, Put and Delete endpoints

import json
import os
from datetime import datetime

import pytz
from bson import ObjectId
from pydantic import BaseModel
from pymongo import MongoClient, ASCENDING, DESCENDING

# Required environment variables
PLANTILLAS_CRUD_HOST = os.environ.get('PLANTILLAS_CRUD_HOST')
PLANTILLAS_CRUD_PORT = os.environ.get('PLANTILLAS_CRUD_PORT')
PLANTILLAS_CRUD_USERNAME = os.environ.get('PLANTILLAS_CRUD_USERNAME')
PLANTILLAS_CRUD_PASS = os.environ.get('PLANTILLAS_CRUD_PASS')
PLANTILLAS_CRUD_DB = os.environ.get('PLANTILLAS_CRUD_DB')
TIMEZONE = os.environ.get('TIMEZONE')
COLLECTION = "tipo_plantilla"

ORDER_LABEL = {
    "desc": DESCENDING,
    "asc": ASCENDING
}


def local_now():
    """Datetime por Timezone"""
    return datetime.now(tz=pytz.timezone(TIMEZONE))


class TipoPlantillaModel(BaseModel):
    """Modelo de datos de TipoPlantilla"""   
    nombre: str
    descripcion: str
    codigo_abreviacion: str


# Gestión de conexión con la BD
def connect_db_client():
    """Genera el cliente para establecer la conexión con la base de datos"""
    try:
        # With password
        if PLANTILLAS_CRUD_USERNAME and PLANTILLAS_CRUD_PASS:
            uri = f"mongodb://{PLANTILLAS_CRUD_USERNAME}:{PLANTILLAS_CRUD_PASS}@{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/"
        else:
            # Without password
            uri = f"mongodb://{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/"
        
        client = MongoClient(uri, uuidRepresentation='standard')
        print("Successful connection to the database")
        return client
    except Exception as ex:
        print(f"Error connecting to the database: {ex}")
        return None


def close_connect_db(client):
    try:
        print("Closing client DB")
        if client:
            client.close()
    except Exception as ex:
        print(f"Error close Client DB. Detail: {ex}")


# Deserialización de parámetros de entrada
# parse_body -> body de las peticiones POST, PUT, DELETE
def parse_body(event) -> tuple:
    try:
        return json.loads(event["body"]), None
    except Exception as ex:
        return None, ex


def get_query(query_str: str) -> dict:
    query_total = {}
    for cond in query_str.split(","):
        kv = cond.split(":", 1)
        if len(kv) == 2:
            k, v = kv
            if v == 'false':
                v = False
            elif v == 'true':
                v = True

            if k == "_id":
                v = ObjectId(v)
        else:
            k, v = kv[0], None
        query_total[k] = v
    return query_total


def get_sort_by(query_params) -> list:
    sort_by_total = []
    if query_params.get("sortby"):
        sort_by_list = str(query_params.get("sortby")).split(",")
        if query_params.get("order"):
            order_list = str(query_params.get("order")).split(",")
            if len(order_list) == 1:
                # Default ASCENDING
                order_label = ORDER_LABEL.get(query_params.get("order"), ASCENDING)
                sort_by_total = [(e, order_label) for e in sort_by_list]
            elif len(order_list) == len(sort_by_list):
                for i, e in enumerate(sort_by_list):
                    order_label = ORDER_LABEL.get(order_list[i], ASCENDING)
                    sort_by_total.append((e, order_label))
            else:
                # Default ASCENDING
                sort_by_total = [(e, ASCENDING) for e in sort_by_list]
    return sort_by_total


def parse_query_params(event) -> tuple:
    try:
        query_params_result = {"limit": 10}
        query_params = event["queryStringParameters"]
        if isinstance(query_params, dict):
            # query: k:v, k: v
            if query_params.get("query"):
                query_params_result["filter"] = get_query(str(query_params.get("query")))

            # fields: col1, col2, entity.col3
            if query_params.get("fields"):
                query_params_result["projection"] = str(query_params.get("fields")).split(",")

            # sortby: col1,col2
            # order: desc,asc
            if query_params.get("sortby"):
                query_params_result["sort"] = get_sort_by(query_params)

            # limit: 10 (default is 10)
            if query_params.get("limit"):
                query_params_result["limit"] = int(query_params.get("limit"))

            # offset: 0 (default is 0)
            if query_params.get("offset"):
                query_params_result["skip"] = int(query_params.get("offset"))

            return query_params_result, None
        else:
            return query_params_result, None
    except Exception as ex:
        print(f"Error in parse_query_params. Detail: {ex}")
        return {}, ex


# Formato de respuestas
def format_specific_values(result):
    if result.get("_id"):
        result["_id"] = str(result["_id"])
    return result


def format_response(result, message: str, status_code: int, success: bool) -> dict:
    """Formats the HTTP response."""
    body = {
        "Success": success,
        "Status": status_code,
        "Message": message
    }
    if success and result is not None:
        if isinstance(result, dict):
            body["Data"] = format_specific_values(result)
        elif isinstance(result, list):
            body["Data"] = [format_specific_values(item) for item in result]
        else:
            body["Data"] = result
    return {"statusCode": status_code, "body": json.dumps(body)}


def create(data, collection):
    try:
        result = collection.insert_one(data)
        if result:
            new_data_id = result.inserted_id
            new_data = collection.find_one(new_data_id)
            return format_response(new_data, "Registration successful", 201, True)
        return format_response({}, "Registration unsuccessful", 400, False)
    except Exception as ex:
        return format_response({}, f"Error service Post: {ex}", 500, False)


def update(_id, data, collection):
    try:
        filter_ = {"_id": ObjectId(_id)}
        result = collection.update_one(filter_, {"$set": data})
        if result.modified_count:
            updated_data = collection.find_one(filter_)
            return format_response(updated_data, "Update successful", 200, True)
        return format_response({}, "Update unsuccessful", 400, False)
    except Exception as ex:
        return format_response({}, f"Error service Put: {ex}", 500, False)


def delete(_id, collection):
    try:
        filter_ = {"_id": ObjectId(_id)}
        data = collection.find_one(filter_)
        if data:
            result = collection.delete_one(filter_)
            if result.deleted_count:
                return format_response(data, "Delete successful", 200, True)
        return format_response(None, "Delete unsuccessful", 400, False)
    except Exception as ex:
        return format_response({}, f"Error service Delete: {ex}", 500, False)


def get_all(query, collection):
    try:
        data = list(collection.find(**query))
        if data:
            return format_response(data, "Request successful", 200, True)
        return format_response([], "Request successful", 200, True)
    except Exception as ex:
        return format_response({}, f"Error service GetAll: {ex}", 500, False)


def get_one(_id, collection):
    try:
        data = collection.find_one({"_id": ObjectId(_id)})
        if data:
            return format_response(data, "Request successful", 200, True)
        return format_response({}, "Request unsuccessful", 404, False)
    except Exception as ex:
        return format_response({}, f"Error service GetOne: {ex}", 500, False)


def lambda_handler(event, context):
    client = None
    try:
        http_method = event['httpMethod']

        if http_method == 'POST':
            data, error = parse_body(event)
            if error is None:
                # Validate structure
                tipo_plantilla_data = TipoPlantillaModel(**data).__dict__
                client = connect_db_client()
                if client:
                    tipo_plantilla_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                    response = create(tipo_plantilla_data, tipo_plantilla_collection)
                    close_connect_db(client)
                    return response
                return format_response({}, "Error registering new tipo_plantilla!", 500, False)
            else:
                return format_response({}, "Error registering new tipo_plantilla! Detail: Error in input data", 500, False)
            
        elif http_method == 'PUT':
            data, error = parse_body(event)
            if error is None:
                # Validate structure
                tipo_plantilla_id = event["pathParameters"]["id"]
                tipo_plantilla_data = TipoPlantillaModel(**data).__dict__
                client = connect_db_client()
                if client:
                    tipo_plantilla_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                    response = update(tipo_plantilla_id, tipo_plantilla_data, tipo_plantilla_collection)
                    close_connect_db(client)
                    return response
                return format_response({}, "Error updating tipo_plantilla!", 500, False)
            else:
                return format_response(error, "Error updating tipo_plantilla! Detail: Error in input data", 500, False)
            
        elif http_method == 'DELETE':
            tipo_plantilla_id = event["pathParameters"]["id"]
            client = connect_db_client()
            if client:
                tipo_plantilla_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                response = delete(tipo_plantilla_id, tipo_plantilla_collection)
                close_connect_db(client)
                return response
            return format_response(None, "Error deleting tipo_plantilla!", 500, False)
        
        elif http_method == 'GET':
            client = connect_db_client()
            if client:
                tipo_plantilla_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                if 'pathParameters' in event and event['pathParameters'] is not None:
                    _id = event["pathParameters"]["id"]
                    response = get_one(_id, tipo_plantilla_collection)
                    close_connect_db(client)
                    return response
                else:
                    query_complement, err = parse_query_params(event)
                    if err is None:
                        response = get_all(query_complement, tipo_plantilla_collection)
                        close_connect_db(client)
                        return response
                    else:
                        return format_response(
                            {},
                            "Error service GetAll: The request contains an incorrect parameter or no record exists",
                            404,
                            True)
            return format_response({}, "Error getting tipo_plantilla!", 500, False)
        
        else:
            close_connect_db(client)
            return format_response({}, f"HTTP method not allowed", 500, False)
    except Exception as ex:
        close_connect_db(client)
        return format_response({}, f"Error in tipo_plantilla request! Detail: {ex}", 500, False)
