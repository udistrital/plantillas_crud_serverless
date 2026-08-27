# Plantillas CRUD Lambda

PLANTILLAS CRUD empleando con AWS SAM.

## Versiones de la API

El servicio expone dos versiones sobre el mismo API Gateway y la misma base de datos.

### v1 — Plantillas

Modelo original. Disponible en las rutas sin prefijo (se conservan para no romper
los consumidores actuales) y también bajo `/v1`.

| Recurso          | Rutas                                                        |
|------------------|--------------------------------------------------------------|
| `plantilla`      | `/plantilla`, `/v1/plantilla` (+ `/{id}`)                    |
| `tipo_plantilla` | `/tipo_plantilla`, `/v1/tipo_plantilla` (+ `/{id}`)          |

### v2 — Versionamiento controlado

Portado desde [plantilla_vc_crud_v2](https://github.com/udistrital/plantilla_vc_crud_v2)
(Go/Beego + PostgreSQL) a Python sobre MongoDB. Cada recurso soporta
`POST /`, `GET /`, `GET /{id}`, `PUT /{id}` y `DELETE /{id}`.

| Recurso             | Ruta                        | Descripción                                    |
|---------------------|-----------------------------|------------------------------------------------|
| `plantilla`         | `/v2/plantilla`             | Plantilla versionable                          |
| `plantilla_version` | `/v2/plantilla_version`     | Versión de una plantilla (`version`, `estado`)  |
| `estructura`        | `/v2/estructura`            | Árbol de composición de una versión            |
| `campo`             | `/v2/campo`                 | Fragmento reutilizable (`tipo`, `html`, `css`) |

Relaciones: `plantilla` → `plantilla_version` → `estructura` → `campo`, donde
`estructura` se referencia a sí misma (`estructura_padre`) para formar el árbol.

**Expansión de referencias.** `GET` de `plantilla_version` y `estructura`
devuelve las referencias expandidas además del identificador, equivalente a
`RelatedSel()` del ORM de Beego:

```json
{
  "Success": true,
  "Status": 200,
  "Message": "Request successful",
  "Data": {
    "_id": "66c5f1a8e4b09d2f3c8a1b7e",
    "version": 2,
    "estado": 1,
    "activo": true,
    "fecha_creacion": "2026-08-21 13:44:00",
    "plantilla_id": "66c5f1a8e4b09d2f3c8a1b01",
    "plantilla": {
      "_id": "66c5f1a8e4b09d2f3c8a1b01",
      "nombre": "Acta de grado",
      "activo": true
    }
  }
}
```

En `estructura` las expansiones son `plantilla_version`, `campo` y `padre`.
En la escritura las referencias se envían siempre como identificadores
(`"plantilla_id": "66c5..."`).

**Integridad referencial.** MongoDB no tiene llaves foráneas, así que respecto
al modelo relacional original:

* `UNIQUE (plantilla_id, version)` la garantiza un índice único de la colección.
* `CHECK (version > 0)` y `CHECK (orden >= 0)` los validan Pydantic en la entrada
  y un `$jsonSchema` en la colección.
* Las llaves foráneas se verifican en el handler antes de escribir: una
  referencia inexistente responde `400`.
* `DELETE` es borrado lógico (`activo: false`) en los cuatro recursos, para que
  no queden referencias huérfanas.
* `PUT` sobre `estructura` rechaza los ciclos en el árbol (`400`).

## Especificaciones Técnicas

### Tecnologías Implementadas y Versiones
* [Python 3.12](https://docs.python.org/3.10/)
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
PLANTILLAS_AUTH_DB=[auth de la base de datos]
TIMEZONE=[zona horaria]
```

**Nota:**
* Por defecto se asignó "America/Bogota", para ver más opciones vea [Lista de zona horarias](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)


### Preparación de la Base de Datos (v2)

Las colecciones de v2 requieren sus índices y validadores antes del primer uso.
El script es idempotente, se puede volver a ejecutar sin efectos secundarios.

```shell
mongosh "mongodb://<usuario>:<password>@<host>:<puerto>/?authSource=<authdb>" \
  --file database/v2_plantilla_vc.js
```

**Nota:**
* El índice único `(plantilla_id, version)` es la restricción que sostiene el
  versionamiento. Sin ejecutar este script el servicio funciona, pero esa
  garantía queda únicamente en la verificación previa del handler, que es
  susceptible a condiciones de carrera entre invocaciones concurrentes.
* La base de datos se toma de `PLANTILLAS_CRUD_DB`, o `plantillas` por defecto.

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

### v1
![Modelo de datos plantillas](/database/plantillas_crud.png)

### v2
Definición de colecciones, validadores e índices en
[`database/v2_plantilla_vc.js`](/database/v2_plantilla_vc.js), que incluye la
correspondencia con el esquema relacional `plantilla_vc` del que se portó.

| Colección                        | Campos                                                                                  |
|----------------------------------|-----------------------------------------------------------------------------------------|
| `plantilla_vc_plantilla`         | `nombre`, `descripcion`, `activo`, `fecha_creacion`, `fecha_modificacion`                |
| `plantilla_vc_campo`             | `tipo`, `html`, `css`, `activo`, `fecha_creacion`, `fecha_modificacion`                   |
| `plantilla_vc_plantilla_version` | `version`, `estado`, `plantilla_id`, `activo`, `fecha_creacion`, `fecha_modificacion`     |
| `plantilla_vc_estructura`        | `orden`, `plantilla_version_id`, `campo_id`, `estructura_padre`, `activo`, `fecha_*`      |

El prefijo `plantilla_vc_` evita la colisión con la colección `plantilla` de v1,
que tiene un modelo distinto.

## Licencia
