
# Get one plantilla

import json
import os

# from bson import ObjectId
from pymongo import MongoClient

PLANTILLAS_CRUD_HOST = os.environ.get('PLANTILLAS_CRUD_HOST')
PLANTILLAS_CRUD_PORT = os.environ.get('PLANTILLAS_CRUD_PORT')
PLANTILLAS_CRUD_USERNAME = os.environ.get('PLANTILLAS_CRUD_USERNAME')
PLANTILAS_CRUD_PASS = os.environ.get('PLANTILAS_CRUD_PASS')
PLANTILLAS_CRUD_DB = os.environ.get('PLANTILLAS_CRUD_DB')


def connect_db_client():
    try:
        # With password
        if PLANTILLAS_CRUD_USERNAME and PLANTILAS_CRUD_PASS:
            uri = f"mongodb+srv://{PLANTILLAS_CRUD_USERNAME}:{PLANTILAS_CRUD_PASS}@{PLANTILLAS_CRUD_HOST}/{PLANTILLAS_CRUD_DB}?retryWrites=true&w=majority"
        else:
            # Without password
            uri = f"mongodb+srv://{PLANTILLAS_CRUD_HOST}/{PLANTILLAS_CRUD_DB}?retryWrites=true&w=majority"

        client = MongoClient(uri)
        print("Client DB Successful")
        return client
    except Exception as ex:
        print("Error Client DB")
        print(f"Detail: {ex}")
        return None


def close_connect_db(client):
    try:
        print("Closing client DB")
        if client:
            client.close()
    except Exception as ex:
        print("Error close Client DB")
        print(f"Detail: {ex}")


def format_specific_values(result):
    if result.get("_id"):
        result["_id"] = str(result["_id"])
    if result.get("fechaCreacion"):
        result["fechaCreacion"] = str(result["fechaCreacion"])
    if result.get("fechaModificacion"):
        result["fechaModificacion"] = str(result["fechaModificacion"])
    return result


def format_response(result, message: str, status_code: int, success: bool):
    if isinstance(result, dict):
        if success:
            result = format_specific_values(result)
            return {"statusCode": status_code,
                    "body": json.dumps({
                        "Success": success,
                        "Status": status_code,
                        "Message": message,
                        "Data": result
                    })}
        else:
            return {"statusCode": status_code,
                    "body": json.dumps({
                        "Success": success,
                        "Status": status_code,
                        "Message": message
                    })}
    if isinstance(result, list):
        result_total = []
        if success:
            for res in result:
                result_total.append(format_specific_values(res))
            return {"statusCode": status_code,
                    "body": json.dumps({
                        "Success": success,
                        "Status": status_code,
                        "Message": message,
                        "Data": result_total
                    })}
        else:
            return {"statusCode": status_code,
                    "body": json.dumps({
                        "Success": success,
                        "Status": status_code,
                        "Message": message
                    })}


def lambda_handler(event, context):
    client = None
    try:
        client = connect_db_client()
        if client:
            print("Connecting database ...")
            plantilla_collection = client["plantillas_bd_pruebas"]["Plantilla"]
            print("Connection database successful")
            plantilla = list(plantilla_collection.find({}))
            print(f"Consulted record.")
            if plantilla:
                print("plantilla found!")
                print(plantilla)
                return format_response(
                    plantilla,
                    "plantilla OK",
                    200,
                    True)
            else:
                print("plantilla not found!")
                close_connect_db(client)
        return format_response(
            {},
            "Error get plantilla! 1",
            403,
            False)
    except Exception as ex:
        print("Error get plantilla 2")
        print(f"Detail: {ex}")
        return format_response(
            {},
            "Error get plantilla 3!",
            403,
            False)
