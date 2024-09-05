# Health Check to API Gateway and lambda
import json

def lambda_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({
            "Success": True,
            "Status": 200,
            "Message": "API CRUD Plantillas"
        })
    }
