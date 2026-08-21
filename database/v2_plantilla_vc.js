// Modelo de datos v2 - Versionamiento controlado de plantillas
//
// Equivalente en MongoDB del esquema relacional `plantilla_vc` definido en
// database/modelo.sql del servicio plantilla_vc_crud_v2.
//
// Uso (idempotente, se puede volver a ejecutar):
//   mongosh "mongodb://<user>:<pass>@<host>:<port>/?authSource=<authdb>" \
//     --file database/v2_plantilla_vc.js
//
// Correspondencia con el modelo relacional:
//
//   plantilla_vc.plantilla          -> plantilla_vc_plantilla
//   plantilla_vc.campo              -> plantilla_vc_campo
//   plantilla_vc.plantilla_version  -> plantilla_vc_plantilla_version
//   plantilla_vc.estructura         -> plantilla_vc_estructura
//
//   INTEGER PRIMARY KEY   -> _id (ObjectId)
//   UNIQUE / CHECK        -> índice único y $jsonSchema (aquí)
//   FOREIGN KEY           -> validación en los handlers antes de escribir,
//                            más borrado lógico (activo: false) en lugar de
//                            borrado físico, para no dejar referencias huérfanas

const DB_NAME = process.env.PLANTILLAS_CRUD_DB || "plantillas";
const target = db.getSiblingDB(DB_NAME);

const FECHAS = {
  fecha_creacion: { bsonType: "date" },
  fecha_modificacion: { bsonType: ["date", "null"] },
};

const ESQUEMAS = {
  plantilla_vc_plantilla: {
    bsonType: "object",
    required: ["fecha_creacion", "nombre", "activo"],
    properties: {
      ...FECHAS,
      nombre: { bsonType: "string", maxLength: 255 },
      descripcion: { bsonType: ["string", "null"] },
      activo: { bsonType: "bool" },
    },
  },

  plantilla_vc_campo: {
    bsonType: "object",
    required: ["fecha_creacion", "tipo", "activo"],
    properties: {
      ...FECHAS,
      tipo: { bsonType: "string", maxLength: 100 },
      html: { bsonType: ["string", "null"] },
      css: { bsonType: ["string", "null"] },
      activo: { bsonType: "bool" },
    },
  },

  plantilla_vc_plantilla_version: {
    bsonType: "object",
    required: ["fecha_creacion", "activo", "version", "estado", "plantilla_id"],
    properties: {
      ...FECHAS,
      // CHECK (version > 0)
      version: { bsonType: ["int", "long"], minimum: 1 },
      estado: { bsonType: ["int", "long"] },
      plantilla_id: { bsonType: "objectId" },
      activo: { bsonType: "bool" },
    },
  },

  plantilla_vc_estructura: {
    bsonType: "object",
    required: ["fecha_creacion", "activo", "orden", "plantilla_version_id"],
    properties: {
      ...FECHAS,
      // CHECK (orden >= 0)
      orden: { bsonType: ["int", "long"], minimum: 0 },
      plantilla_version_id: { bsonType: "objectId" },
      campo_id: { bsonType: ["objectId", "null"] },
      estructura_padre: { bsonType: ["objectId", "null"] },
      activo: { bsonType: "bool" },
    },
  },
};

const INDICES = {
  plantilla_vc_plantilla: [
    { keys: { activo: 1 }, options: { name: "ix_plantilla_activo" } },
  ],

  plantilla_vc_campo: [
    { keys: { activo: 1 }, options: { name: "ix_campo_activo" } },
  ],

  plantilla_vc_plantilla_version: [
    // UNIQUE (plantilla_id, version). Es la restricción que sostiene el
    // versionamiento: la garantiza el motor, no el código.
    {
      keys: { plantilla_id: 1, version: 1 },
      options: { name: "uq_plantilla_version", unique: true },
    },
  ],

  plantilla_vc_estructura: [
    { keys: { plantilla_version_id: 1 }, options: { name: "ix_estructura_plantilla_version" } },
    { keys: { estructura_padre: 1 }, options: { name: "ix_estructura_padre" } },
    { keys: { campo_id: 1 }, options: { name: "ix_estructura_campo" } },
  ],
};

const existentes = target.getCollectionNames();

Object.keys(ESQUEMAS).forEach(function (nombre) {
  const validator = { $jsonSchema: ESQUEMAS[nombre] };

  if (existentes.indexOf(nombre) === -1) {
    target.createCollection(nombre, { validator: validator, validationLevel: "strict" });
    print("createCollection  " + nombre);
  } else {
    target.runCommand({ collMod: nombre, validator: validator, validationLevel: "strict" });
    print("collMod           " + nombre);
  }

  (INDICES[nombre] || []).forEach(function (indice) {
    target.getCollection(nombre).createIndex(indice.keys, indice.options);
    print("createIndex       " + nombre + "." + indice.options.name);
  });
});

print("\nModelo v2 listo en la base de datos '" + DB_NAME + "'.");
