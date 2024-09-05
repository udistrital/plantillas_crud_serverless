# Plantillas CRUD Lambda

PLANTILLAS CRUD empleando con AWS SAM.

## Especificaciones Técnicas

### Tecnologías Implementadas y Versiones
* [Python 3.10](https://docs.python.org/3.10/)
* [AWS SAM](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli.html)
* [AWS SAM CLI](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/install-sam-cli.html)
* Opcional (Requerido para ejecutar el servicio API en local, simula el API Gateway) [Docker](https://docs.docker.com/engine/install/ubuntu/)
* Opcional (Gestión de versiones de librerias en local) [virtualenv](https://virtualenv.pypa.io/en/latest/installation.html)


### Variables de Entorno
```shell
PLANTILLAS_CRUD_HOST=[direccion de la base de datos]
PLANTILLAS_CRUD_PORT=[Puerto de conexión con la base de datos]
PLANTILLAS_CRUD_USERNAME=[usuario con acceso a la base de datos]
PLANTILLAS_CRUD_PASS=[password del usuario]
PLANTILLAS_CRUD_DB=[nombre de la base de datos]
TIMEZONE=[zona horaria]
```

**Nota:**
* Por defecto se asignó "America/Bogota", para ver más opciones vea [Lista de zona horarias](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)


### Ejecución del Proyecto en Local
```shell
sam build
sam local start-api --env-vars env.json
```
**Nota:**
* Para más detalle de las formas de ejecutarlo localmente vea [Uso sam local](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli-local.html)
* Puede usar el script `run_local.sh` para correr los comandos indicados anteriormente con bash. 

### Ejecución Pruebas

Pruebas unitarias
```shell
# En Proceso
```

### Despliegue
```shell
sam build
sam deploy --guided
```
**Nota:** 
* Para mayor información para realizar el despliegue vea [Uso sam deploy](https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/using-sam-cli-deploy.html).

## Estado CI

## Modelo de Datos
![Modelo de datos plantillas](/database/plantillas_crud.png)

## Licencia
