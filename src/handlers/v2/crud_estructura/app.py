# CRUD ESTRUCTURA (v2)
# Get one, Get All, Post, Put and Delete endpoints

import json
import os
from datetime import datetime
from typing import Optional

import pytz
from bson import ObjectId
from pydantic import BaseModel, Field, field_validator
from pymongo import MongoClient, ASCENDING, DESCENDING

# Required environment variables
PLANTILLAS_CRUD_HOST = os.environ.get('PLANTILLAS_CRUD_HOST')
PLANTILLAS_CRUD_PORT = os.environ.get('PLANTILLAS_CRUD_PORT')
PLANTILLAS_CRUD_USERNAME = os.environ.get('PLANTILLAS_CRUD_USERNAME')
PLANTILLAS_CRUD_PASS = os.environ.get('PLANTILLAS_CRUD_PASS')
PLANTILLAS_CRUD_DB = os.environ.get('PLANTILLAS_CRUD_DB')
PLANTILLAS_AUTH_DB = os.environ.get('PLANTILLAS_AUTH_DB')
TIMEZONE = os.environ.get('TIMEZONE')
COLLECTION = "plantilla_vc_estructura"
PLANTILLA_VERSION_COLLECTION = "plantilla_vc_plantilla_version"
CAMPO_COLLECTION = "plantilla_vc_campo"

# Formato de fecha del contrato v2 (equivale a "2006-01-02 15:04:05" en Go)
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Tope del recorrido de ancestros al validar ciclos en el árbol
MAX_TREE_DEPTH = 100

ORDER_LABEL = {
    "desc": DESCENDING,
    "asc": ASCENDING
}

# Campos que requieren conversión de tipo en el parámetro query
INT_FIELDS = ['orden']
OBJECT_ID_FIELDS = ['_id', 'plantilla_version_id', 'campo_id', 'estructura_padre']

# Referencias que en el modelo relacional eran llaves foráneas.
# Se expanden en las consultas, equivalente a RelatedSel() del ORM de Beego
LOOKUPS = [
    {"local_field": "plantilla_version_id", "collection": PLANTILLA_VERSION_COLLECTION,
     "alias": "plantilla_version"},
    {"local_field": "campo_id", "collection": CAMPO_COLLECTION, "alias": "campo"},
    {"local_field": "estructura_padre", "collection": COLLECTION, "alias": "padre"},
]

# Referencias a validar en la escritura (reemplazan las llaves foráneas)
REFERENCES = [
    {"field": "plantilla_version_id", "collection": PLANTILLA_VERSION_COLLECTION},
    {"field": "campo_id", "collection": CAMPO_COLLECTION},
    {"field": "estructura_padre", "collection": COLLECTION},
]


def local_now():
    """Datetime por Timezone"""
    return datetime.now(tz=pytz.timezone(TIMEZONE))


def validate_optional_object_id(value, field_name):
    if value is not None and not ObjectId.is_valid(value):
        raise ValueError(f"{field_name} must be a valid ObjectId")
    return value


class EstructuraModel(BaseModel):
    """Modelo de datos de Estructura (v2)"""
    plantilla_version_id: str
    orden: int = Field(ge=0)
    estructura_padre: Optional[str] = None
    campo_id: Optional[str] = None
    activo: bool = Field(default=True)

    @field_validator('plantilla_version_id')
    @classmethod
    def validate_plantilla_version_id(cls, value):
        if not ObjectId.is_valid(value):
            raise ValueError("plantilla_version_id must be a valid ObjectId")
        return value

    @field_validator('estructura_padre')
    @classmethod
    def validate_estructura_padre(cls, value):
        return validate_optional_object_id(value, "estructura_padre")

    @field_validator('campo_id')
    @classmethod
    def validate_campo_id(cls, value):
        return validate_optional_object_id(value, "campo_id")


class EstructuraCreationModel(EstructuraModel):
    fecha_creacion: datetime = Field(default_factory=local_now)
    fecha_modificacion: datetime = Field(default_factory=local_now)


class EstructuraUpdateModel(EstructuraModel):
    """fecha_creacion no se expone en el update para no reescribir el histórico"""
    fecha_modificacion: datetime = Field(default_factory=local_now)


class DeleteEstructuraModel(BaseModel):
    activo: bool = Field(default=False)
    fecha_modificacion: datetime = Field(default_factory=local_now)


# Gestión de conexión con la BD
def connect_db_client():
    """Genera el cliente para establecer la conexión con la base de datos"""
    try:
        # With password
        if PLANTILLAS_CRUD_USERNAME and PLANTILLAS_CRUD_PASS:
            uri = f"mongodb://{PLANTILLAS_CRUD_USERNAME}:{PLANTILLAS_CRUD_PASS}@{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/?authSource={PLANTILLAS_AUTH_DB}"
        else:
            # Without password
            uri = f"mongodb://{PLANTILLAS_CRUD_HOST}:{PLANTILLAS_CRUD_PORT}/"

        client = MongoClient(uri, uuidRepresentation='standard', tz_aware=True)
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


def parse_model(model, data) -> tuple:
    """Valida la estructura de entrada contra el modelo de datos"""
    try:
        return model(**data).__dict__, None
    except Exception as ex:
        return None, ex


def to_document(data: dict) -> dict:
    """Convierte los identificadores de referencia a ObjectId antes de persistir"""
    document = dict(data)
    for reference in REFERENCES:
        if document.get(reference["field"]):
            document[reference["field"]] = ObjectId(document[reference["field"]])
    return document


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
            elif v == 'null':
                v = None

            if v is not None:
                if k in INT_FIELDS:
                    v = int(v)
                elif k in OBJECT_ID_FIELDS:
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


# Integridad referencial: reemplaza las llaves foráneas del modelo relacional
def validate_references(data: dict, db) -> Optional[str]:
    """Verifica que existan plantilla_version, campo y estructura_padre referenciados"""
    for reference in REFERENCES:
        value = data.get(reference["field"])
        if value and not db[reference["collection"]].find_one({"_id": value}, {"_id": 1}):
            return f"{reference['field']} {value} does not exist"
    return None


def validate_no_cycle(_id, parent_id, collection) -> Optional[str]:
    """Evita ciclos en el árbol de estructura.
    El modelo relacional tampoco los impedía (una FK a sí misma los permite),
    pero un ciclo rompe cualquier recorrido jerárquico de la plantilla.
    """
    if _id is None or parent_id is None:
        return None
    if ObjectId(_id) == parent_id:
        return "estructura_padre cannot reference itself"

    current = parent_id
    for _ in range(MAX_TREE_DEPTH):
        node = collection.find_one({"_id": current}, {"estructura_padre": 1})
        if node is None or node.get("estructura_padre") is None:
            return None
        if node["estructura_padre"] == ObjectId(_id):
            return "estructura_padre would create a cycle in the tree"
        current = node["estructura_padre"]
    return f"estructura_padre exceeds the maximum tree depth ({MAX_TREE_DEPTH})"


# Consultas con expansión de referencias
def build_lookup_stages() -> list:
    """Expande las referencias, equivalente a RelatedSel() del ORM de Beego"""
    stages = []
    for lookup in LOOKUPS:
        stages.append({"$lookup": {
            "from": lookup["collection"],
            "localField": lookup["local_field"],
            "foreignField": "_id",
            "as": lookup["alias"]
        }})
        stages.append({"$unwind": {"path": f"${lookup['alias']}", "preserveNullAndEmptyArrays": True}})
    return stages


def build_pipeline(query: dict) -> list:
    """Arma el pipeline de agregación. El filtrado y la paginación van antes de
    los $lookup para expandir solo los documentos que se van a devolver.
    """
    pipeline = []
    if query.get("filter"):
        pipeline.append({"$match": query["filter"]})
    if query.get("sort"):
        pipeline.append({"$sort": dict(query["sort"])})
    if query.get("skip"):
        pipeline.append({"$skip": query["skip"]})
    if query.get("limit"):
        pipeline.append({"$limit": query["limit"]})
    pipeline.extend(build_lookup_stages())
    if query.get("projection"):
        pipeline.append({"$project": {field: 1 for field in query["projection"]}})
    return pipeline


# Formato de respuestas
def format_document(value):
    """Serializa ObjectId y datetime de forma recursiva para la respuesta JSON"""
    if isinstance(value, list):
        return [format_document(item) for item in value]
    if isinstance(value, dict):
        return {k: format_document(v) for k, v in value.items()}
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = pytz.utc.localize(value)
        return value.astimezone(pytz.timezone(TIMEZONE)).strftime(DATETIME_FORMAT)
    return value


def format_response(result, message: str, status_code: int, success: bool) -> dict:
    """Formats the HTTP response."""
    body = {
        "Success": success,
        "Status": status_code,
        "Message": message
    }
    if success and result is not None:
        body["Data"] = format_document(result)
    return {"statusCode": status_code, "body": json.dumps(body)}


def create(data, db):
    try:
        print("[Estructura v2] Create: ", data)
        collection = db[COLLECTION]

        error = validate_references(data, db)
        if error:
            return format_response({}, f"Error service Post: {error}", 400, False)

        result = collection.insert_one(data)
        if result:
            new_data = get_expanded_one(result.inserted_id, collection)
            return format_response(new_data, "Registration successful", 201, True)
        return format_response({}, "Registration unsuccessful", 400, False)
    except Exception as ex:
        print(f"[Estructura v2] Error service Post: {ex}")
        return format_response({}, f"Error service Post: {ex}", 500, False)


def update(_id, data, db):
    try:
        print("[Estructura v2] Update: ", _id)
        collection = db[COLLECTION]
        filter_ = {"_id": ObjectId(_id)}

        error = validate_references(data, db)
        if error:
            return format_response({}, f"Error service Put: {error}", 400, False)

        error = validate_no_cycle(_id, data.get("estructura_padre"), collection)
        if error:
            return format_response({}, f"Error service Put: {error}", 400, False)

        result = collection.update_one(filter_, {"$set": data})
        if result.matched_count:
            updated_data = get_expanded_one(ObjectId(_id), collection)
            return format_response(updated_data, "Update successful", 200, True)
        return format_response({}, "Update unsuccessful", 400, False)
    except Exception as ex:
        print(f"[Estructura v2] Error service Put: {ex}")
        return format_response({}, f"Error service Put: {ex}", 500, False)


def delete(_id, data, collection):
    """Borrado lógico: preserva la integridad referencial con los nodos hijos"""
    try:
        print("[Estructura v2] Delete: ", _id)
        filter_ = {"_id": ObjectId(_id)}
        result = collection.update_one(filter_, {"$set": data})
        if result.matched_count:
            updated_data = get_expanded_one(ObjectId(_id), collection)
            return format_response(updated_data, "Delete successful", 200, True)
        return format_response(None, "Delete unsuccessful", 400, False)
    except Exception as ex:
        print(f"[Estructura v2] Error service Delete: {ex}")
        return format_response({}, f"Error service Delete: {ex}", 500, False)


def get_expanded_one(_id, collection):
    """Lee un documento con sus referencias expandidas"""
    pipeline = [{"$match": {"_id": _id}}] + build_lookup_stages()
    data = list(collection.aggregate(pipeline))
    return data[0] if data else None


def get_all(query, collection):
    try:
        print("[Estructura v2] GetAll: ", query)
        data = list(collection.aggregate(build_pipeline(query)))
        if data:
            print("[Estructura v2] GetAll result: ", data)
            return format_response(data, "Request successful", 200, True)
        return format_response([], "Request successful", 200, True)
    except Exception as ex:
        print(f"[Estructura v2] Error service GetAll: {ex}")
        return format_response({}, f"Error service GetAll: {ex}", 500, False)


def get_one(_id, collection):
    try:
        print("[Estructura v2] GetOne: find by id ", _id)
        data = get_expanded_one(ObjectId(_id), collection)
        if data:
            print("[Estructura v2] GetOne result: ", data)
            return format_response(data, "Request successful", 200, True)
        return format_response({}, "Request unsuccessful", 404, False)
    except Exception as ex:
        print(f"[Estructura v2] Error service GetOne: {ex}")
        return format_response({}, f"Error service GetOne: {ex}", 500, False)


def lambda_handler(event, context):
    client = None
    try:
        http_method = event['httpMethod']

        if http_method == 'POST':
            data, error = parse_body(event)
            if error is None:
                # Validate structure
                estructura_data, error = parse_model(EstructuraCreationModel, data)
                if error is not None:
                    return format_response({}, f"Error registering new estructura! Detail: {error}", 400, False)
                client = connect_db_client()
                if client:
                    db = client[str(PLANTILLAS_CRUD_DB)]
                    response = create(to_document(estructura_data), db)
                    close_connect_db(client)
                    return response
                return format_response({}, "Error registering new estructura!", 500, False)
            else:
                return format_response({}, "Error registering new estructura! Detail: Error in input data", 400, False)

        elif http_method == 'PUT':
            data, error = parse_body(event)
            if error is None:
                # Validate structure
                estructura_id = event["pathParameters"]["id"]
                estructura_data, error = parse_model(EstructuraUpdateModel, data)
                if error is not None:
                    return format_response({}, f"Error updating estructura! Detail: {error}", 400, False)
                client = connect_db_client()
                if client:
                    db = client[str(PLANTILLAS_CRUD_DB)]
                    response = update(estructura_id, to_document(estructura_data), db)
                    close_connect_db(client)
                    return response
                return format_response({}, "Error updating estructura!", 500, False)
            else:
                return format_response({}, "Error updating estructura! Detail: Error in input data", 400, False)

        elif http_method == 'DELETE':
            estructura_id = event["pathParameters"]["id"]
            estructura_data = DeleteEstructuraModel().__dict__
            client = connect_db_client()
            if client:
                estructura_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                response = delete(estructura_id, estructura_data, estructura_collection)
                close_connect_db(client)
                return response
            return format_response(None, "Error deleting estructura!", 500, False)

        elif http_method == 'GET':
            client = connect_db_client()
            if client:
                estructura_collection = client[str(PLANTILLAS_CRUD_DB)][COLLECTION]
                if 'pathParameters' in event and event['pathParameters'] is not None:
                    _id = event["pathParameters"]["id"]
                    response = get_one(_id, estructura_collection)
                    close_connect_db(client)
                    return response
                else:
                    query_complement, err = parse_query_params(event)
                    if err is None:
                        response = get_all(query_complement, estructura_collection)
                        close_connect_db(client)
                        return response
                    else:
                        return format_response(
                            {},
                            "Error service GetAll: The request contains an incorrect parameter or no record exists",
                            404,
                            True)
            return format_response({}, "Error getting estructura!", 500, False)

        else:
            close_connect_db(client)
            return format_response({}, "HTTP method not allowed", 500, False)
    except Exception as ex:
        close_connect_db(client)
        return format_response({}, f"Error in estructura request! Detail: {ex}", 500, False)
