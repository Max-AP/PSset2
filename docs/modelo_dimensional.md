# Modelo Dimensional (Esquema Clean)

La capa `clean` del proyecto ha sido diseñada siguiendo un modelo estrella (Star Schema) para optimizar el rendimiento de las consultas analíticas y garantizar una estructura clara y escalable.

## Tabla de Hechos: `fact_trips`

Representa el evento principal: un viaje de taxi completado que ha pasado todos los filtros de calidad.

- **Granularidad**: Un registro por cada viaje individual que cumpla con los criterios técnicos.
- **Clave Primaria**: `trip_key` (generada secuencialmente mediante `clean.fact_trips_trip_key_seq`).
- **Claves Foráneas**:
  - `vendor_key` (referencia a `dim_vendor`).
  - `payment_key` (referencia a `dim_payment_type`).
  - `pickup_key` (referencia a `dim_pickup_location`).
  - `dropoff_key` (referencia a `dim_dropoff_location`).

### Métricas y Atributos de Hechos:
- `tpep_pickup_datetime` / `tpep_dropoff_datetime`: Marcas de tiempo del viaje.
- `trip_duration_minutes`: Calculado como (dropoff - pickup) en minutos.
- `trip_distance`: Distancia recorrida (en millas).
- `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`: Montos económicos del servicio.
- `passenger_count`: Cantidad de pasajeros registrados.
- `source_year`, `source_month`: Atributos para facilitar el particionamiento lógico y filtrado.

## Tablas de Dimensiones

Permiten analizar los viajes desde distintos ángulos (quién, cómo, dónde).

### 1. `dim_vendor`
- **vendor_key**: Clave primaria subrogada.
- **vendor_id**: ID original (1: Creative Mobile, 2: VeriFone).
- **vendor_name**: Nombre descriptivo del proveedor.

### 2. `dim_payment_type`
- **payment_key**: Clave primaria subrogada.
- **payment_id**: ID numérico del método de pago.
- **payment_description**: Descripción clara (Credit Card, Cash, etc.).

### 3. `dim_pickup_location`
- **pickup_key**: Clave primaria subrogada.
- **location_id**: ID de la zona de recogida según la TLC.

### 4. `dim_dropoff_location`
- **dropoff_key**: Clave primaria subrogada.
- **location_id**: ID de la zona de entrega según la TLC.

## Nota sobre Dimensiones de Ubicación
Actualmente, las dimensiones de ubicación (`pickup` y `dropoff`) contienen únicamente el `location_id` proporcionado por la fuente original. Para un análisis más profundo, se podría integrar la tabla de referencia `taxi_zone_lookup.csv` de la TLC para incluir nombres de barrios (Borough) y zonas específicas. Debido a que el requerimiento se centraba en la estructura dimensional, se ha priorizado el uso de IDs y claves subrogadas.

---

## Representación del Modelo Estrella (Texto)

```text
    ┌───────────────────────┐             ┌───────────────────────┐
    │      dim_vendor       │             │   dim_payment_type    │
    ├───────────────────────┤             ├───────────────────────┤
    │ vendor_key (PK)       │             │ payment_key (PK)      │
    │ vendor_id             │             │ payment_id            │
    │ vendor_name           │             │ payment_description   │
    └──────────┬────────────┘             └──────────┬────────────┘
               │                                     │
               │         ┌──────────────────┐        │
               │         │    fact_trips    │        │
               │         ├──────────────────┤        │
               └─────────┤ trip_key (PK)    ├────────┘
                         │ vendor_key (FK)  │
                         │ payment_key (FK) │
               ┌─────────┤ pickup_key (FK)  ├────────┐
               │         │ dropoff_key (FK) │        │
               │         │ trip_duration... │        │
               │         │ trip_distance    │        │
               │         │ total_amount...  │        │
               │         └──────────────────┘        │
               │                                     │
    ┌──────────┴────────────┐             ┌──────────┴────────────┐
    │  dim_pickup_location  │             │ dim_dropoff_location  │
    ├───────────────────────┤             ├───────────────────────┤
    │ pickup_key (PK)       │             │ dropoff_key (PK)      │
    │ location_id           │             │ location_id           │
    └───────────────────────┘             └───────────────────────┘
```

## Relaciones y Diagrama (Gráfico)
![Tablas en esquema clean](../screenshots/clean%20tables.png)
*Figura: Estructura de tablas y claves subrogadas en el esquema clean, visualizada en pgAdmin.*
