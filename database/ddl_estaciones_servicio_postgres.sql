
-- DDL para PostgreSQL - BD estaciones

CREATE TABLE empresa (
    id_empresa INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL
);

CREATE TABLE tipo_carburante (
    id_tipo_carburante INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL
);

CREATE TABLE estacion_servicio (
    id_estacion INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_empresa INTEGER NOT NULL,
    tipo_estacion VARCHAR(20) NOT NULL
        CHECK (tipo_estacion IN ('TERRESTRE', 'MARITIMA')),
    provincia VARCHAR(255) NOT NULL,
    municipio VARCHAR(255) NOT NULL,
    localidad VARCHAR(255) NOT NULL,
    codigo_postal VARCHAR(10),
    direccion VARCHAR(255) NOT NULL,
    margen VARCHAR(5),
    latitud DOUBLE PRECISION NOT NULL,
    longitud DOUBLE PRECISION NOT NULL,
    horario VARCHAR(255),
    CONSTRAINT fk_estacion_empresa
        FOREIGN KEY (id_empresa) REFERENCES empresa(id_empresa)
);

CREATE TABLE precio_carburante (
    id_estacion INTEGER NOT NULL,
    id_tipo_carburante INTEGER NOT NULL,
    fecha TIMESTAMP NOT NULL,
    precio NUMERIC(10,3) NOT NULL,
    PRIMARY KEY (id_estacion, id_tipo_carburante, fecha),
    CONSTRAINT fk_precio_estacion
        FOREIGN KEY (id_estacion) REFERENCES estacion_servicio(id_estacion),
    CONSTRAINT fk_precio_tipo_carburante
        FOREIGN KEY (id_tipo_carburante) REFERENCES tipo_carburante(id_tipo_carburante)
);
