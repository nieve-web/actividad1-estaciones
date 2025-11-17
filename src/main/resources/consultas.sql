/* ==============================================================
   1) Empresa con más estaciones TERRESTRES
   ============================================================== */

SELECT e.nombre, COUNT(*) AS total_estaciones
FROM empresa e
JOIN estacion_servicio es ON es.id_empresa = e.id_empresa
WHERE es.tipo_estacion = 'TERRESTRE'
GROUP BY e.nombre
ORDER BY total_estaciones DESC
LIMIT 1;



/* ==============================================================
   2) Empresa con más estaciones MARÍTIMAS
   ============================================================== */

SELECT e.nombre, COUNT(*) AS total_estaciones
FROM empresa e
JOIN estacion_servicio es ON es.id_empresa = e.id_empresa
WHERE es.tipo_estacion = 'MARITIMA'
GROUP BY e.nombre
ORDER BY total_estaciones DESC
LIMIT 1;



/* ==============================================================
   3) Estación más barata para Gasolina 95 E5 en Comunidad de Madrid
   ============================================================== */

SELECT
    es.provincia,
    es.municipio,
    es.localidad,
    es.direccion,
    emp.nombre AS empresa,
    es.margen,
    pc.precio
FROM precio_carburante pc
JOIN estacion_servicio es
    ON pc.id_estacion = es.id_estacion
JOIN empresa emp
    ON es.id_empresa = emp.id_empresa
JOIN tipo_carburante tc
    ON pc.id_tipo_carburante = tc.id_tipo_carburante
WHERE
    tc.nombre = 'Gasolina 95 E5'
    AND es.provincia = 'MADRID'
ORDER BY pc.precio ASC
LIMIT 1;


/* ============================================================
   4. Estación más barata de Gasóleo A a menos de 10 km de Albacete
   ============================================================ */

WITH precios AS (
    SELECT
        es.id_estacion,
        es.provincia,
        es.municipio,
        es.localidad,
        es.direccion,
        es.margen,
        pc.precio,
        (
            6371 * acos(
                cos(radians(38.995548)) * cos(radians(es.latitud)) *
                cos(radians(es.longitud) - radians(-1.855459)) +
                sin(radians(38.995548)) * sin(radians(es.latitud))
            )
        ) AS distancia_km
    FROM estacion_servicio es
    JOIN precio_carburante pc ON pc.id_estacion = es.id_estacion
    JOIN tipo_carburante tc ON tc.id_tipo_carburante = pc.id_tipo_carburante
    WHERE tc.nombre = 'Gasóleo A'
)
SELECT *
FROM precios
WHERE distancia_km <= 10
ORDER BY precio ASC
LIMIT 1;


/* ==============================================================
   5) Provincia con la estación marítima más cara de Gasolina 95 E5
   ============================================================== */

SELECT
    es.provincia,
    pc.precio
FROM precio_carburante pc
JOIN estacion_servicio es
    ON pc.id_estacion = es.id_estacion
JOIN tipo_carburante tc
    ON pc.id_tipo_carburante = tc.id_tipo_carburante
WHERE
    es.tipo_estacion = 'MARITIMA'
    AND tc.nombre = 'Gasolina 95 E5'
ORDER BY pc.precio DESC
LIMIT 1;
