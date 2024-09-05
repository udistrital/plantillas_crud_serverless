# Put plantilla

import json
import os
from datetime import datetime
from typing import List, Dict, Optional

import pytz
from bson import ObjectId
from pydantic import BaseModel, Field
from pymongo import MongoClient
PLANTILLAS_CRUD_HOST = os.environ.get('PLANTILLAS_CRUD_HOST')
PLANTILLAS_CRUD_PORT = os.environ.get('PLANTILLAS_CRUD_PORT')
PLANTILLAS_CRUD_USERNAME = os.environ.get('PLANTILLAS_CRUD_USERNAME')
PLANTILAS_CRUD_PASS = os.environ.get('PLANTILAS_CRUD_PASS')
PLANTILLAS_CRUD_DB = os.environ.get('PLANTILLAS_CRUD_DB')
TIMEZONE = os.environ.get('TIMEZONE')
COLLECTION = "plantilla"

print(PLANTILLAS_CRUD_DB)


def local_now():
    return datetime.now(tz=pytz.timezone(TIMEZONE))
class PlantillaModel(BaseModel):
    id: int
    tipo: str
    nombre: str
    descripcion: str
    contenido: str
    enlace: str
    FechaCreacion: datetime = Field(default=local_now())
    FechaModificacion: Optional[datetime] = None
    version: float
    versionActual: bool


def connect_db_client():
    try:
        # With password
        if PLANTILLAS_CRUD_USERNAME and PLANTILAS_CRUD_PASS:
            uri = f"mongodb://{PLANTILLAS_CRUD_USERNAME}:{PLANTILAS_CRUD_PASS}@" \
                  f"{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/"
        else:
            # Without password
            uri = f"mongodb://{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/"

        client = MongoClient(uri, uuidRepresentation='standard')
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


def parse_body(event) -> tuple:
    try:
        return json.loads(event["body"]), None
    except Exception as ex:
        return None, ex


def format_response(result, message: str, status_code: int, success: bool):
    if isinstance(result, dict):
        if success:
            if result.get("_id"):
                result["_id"] = str(result["_id"])
            if result.get("fechaCreacion"):
                result["fechaCreacion"] = str(result["fechaCreacion"])
            if result.get("fechaModificacion"):
                result["fechaModificacion"] = str(result["fechaModificacion"])

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


def lambda_handler(event, context):
    client = None
    try:
        plantilla_id = event["pathParameters"]["id"]
        print(plantilla_id)
        data, error = parse_body(event)
        if error is None:
            # Validate structure
            plantilla_data = PlantillaModel(**data).__dict__
            print(plantilla_data)
            client = connect_db_client()
            filter_ = {"_id": ObjectId(plantilla_id)}
            if client:
                print("Connecting database ...")
                plantilla_collection = client[str(PLANTILLAS_CRUD_DB)]["plantilla"]
                print("Connection database successful")
                print("Updating plantilla")
                result = plantilla_collection.update_one(
                    filter_,
                    {"$set": plantilla_data})
                print(f"Updated plantilla {plantilla_id}")
                if result:
                    plantilla = plantilla_collection.find_one(filter_)
                    return format_response(
                        plantilla,
                        "Updated plantilla",
                        200,
                        True)
                else:
                    close_connect_db(client)
            return format_response(
                {},
                "Error updating plantilla!",
                403,
                False)
    except Exception as ex:
        print("Error updating plantilla")
        print(f"Detail: {ex}")
        close_connect_db(client)
        return format_response(
            {},
            "Error updating plantilla!",
            403,
            False)
