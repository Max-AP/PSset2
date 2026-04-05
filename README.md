# 🚖 Pipeline de Datos: NYC Taxi

Este proyecto implementa un pipeline de datos integral (end-to-end) para la ingesta, transformación y modelado de datos de viajes en taxi amarillo de la ciudad de Nueva York (Yellow Taxi), orquestado con **Mage AI**, almacenado en **PostgreSQL** y visualizable mediante **pgAdmin**.

---

## 🎯 Objetivo del Proyecto

El objetivo principal es construir una arquitectura de datos robusta y escalable que permita:

1.  **Ingesta Automatizada**: Descarga de datos históricos de viajes en taxi desde el portal oficial de la TLC (Taxi and Limousine Commission).
2.  **Arquitectura de Capas**:
    *   **Capa Raw**: Almacenamiento de datos crudos con mínima transformación.
    *   **Capa Clean**: Transformación de datos mediante un modelo dimensional (estrella) optimizado para análisis y consultas de inteligencia de negocios.
3.  **Monitoreo y Orquestación**: Uso de Mage AI para gestionar las dependencias entre pipelines y programar ejecuciones.
4.  **Visualización y Análisis**: Exposición de los datos modelados en PostgreSQL para su consumo mediante SQL o herramientas de BI.

---

## 🏗️ Arquitectura

El sistema se basa en contenedores Docker, garantizando un entorno reproducible y aislado:

```text
┌─────────────────────────────────────────────────────────┐
│ Docker Compose                                          │
│                                                         │
│ ┌──────────────┐   ┌────────────────┐   ┌──────────┐    │
│ │   Mage AI    │   │   PostgreSQL   │   │  pgAdmin │    │
│ │(Orquestador) │──>│(Data Warehouse)│──>│   (UI)   │    │
│ │  Port: 6789  │   │   Port: 5432   │   │Port: 9000│    │
│ └──────────────┘   └────────────────┘   └──────────┘    │
└─────────────────────────────────────────────────────────┘
```

| Servicio | Puerto | Descripción |
| :--- | :--- | :--- |
| `data-warehouse` | `5432` | Base de datos PostgreSQL (Motor de almacenamiento y modelado). |
| `orquestador` | `6789` | Mage AI (Pipeline de datos, orquestación y triggers). |
| `warehouse-ui` | `9000` | pgAdmin 4 (Interfaz web para administración de la base de datos). |

---

## ⚙️ Pasos para Levantar el Entorno

### 📋 Prerrequisitos

*   [Docker](https://www.docker.com/) y [Docker Compose](https://docs.docker.com/compose/) instalados.
*   Git instalado para clonar el repositorio.

### 🚀 Instalación y Despliegue

1.  **Clonar el repositorio**:
    ```bash
    git clone <URL_DEL_REPOSITORIO>
    cd PSset2
    ```

2.  **Configurar variables de entorno**:
    Asegúrate de tener un archivo `.env` en la raíz del proyecto con el siguiente contenido (puedes ajustarlo según tus preferencias):
    ```env
    POSTGRES_USER=root
    POSTGRES_PASSWORD=root
    POSTGRES_DB=warehouse
    POSTGRES_HOST=data-warehouse
    POSTGRES_PORT=5432
    ```

3.  **Levantar los contenedores**:
    ```bash
    docker-compose up -d
    ```
    *Este comando descargará las imágenes necesarias e iniciará los servicios en segundo plano.*

4.  **Verificar estado**:
    ```bash
    docker-compose ps
    ```

---

## 🔄 Cómo Ejecutar los Pipelines

1.  Accede a la interfaz de **Mage AI** en `http://localhost:6789`.
2.  Navega a la sección **Pipelines**.
3.  **Ejecución de Ingesta (Raw)**:
    *   Selecciona `raw_ingestion_pipeline`.
    *   Ejecuta el pipeline manualmente o crea un *Trigger*.
    *   Este pipeline descarga los datos y los carga en el esquema `raw`.
    *   Al finalizar, el bloque `trigger_clean_pipeline` disparará automáticamente el siguiente pipeline de transformación.
4.  **Ejecución de Transformación (Clean)**:
    *   El pipeline `clean_transformation_pipeline` lee los datos del esquema `raw`.
    *   Aplica limpieza y construye el modelo dimensional en el esquema `clean`.

---

## 🗄️ Cómo Acceder a pgAdmin

1.  Abre en tu navegador `http://localhost:9000`.
2.  **Login**:
    *   **Email**: `enaunay@gmail.com`
    *   **Password**: `root`
3.  **Configurar el Servidor**:
    *   Haz clic derecho en `Servers` > `Register` > `Server...`.
    *   **Name**: `Taxi Warehouse` (o el de tu elección).
    *   En la pestaña **Connection**:
        *   **Host name/address**: `data-warehouse` (nombre del servicio en docker-compose).
        *   **Port**: `5432`
        *   **Maintenance database**: `warehouse` (o el valor de `POSTGRES_DB`).
        *   **Username**: `root` (o el valor de `POSTGRES_USER`).
        *   **Password**: `root` (o el valor de `POSTGRES_PASSWORD`).
    *   Haz clic en **Save**.

---

## ✅ Cómo Validar Resultados en PostgreSQL

Puedes validar que los datos se cargaron correctamente ejecutando las siguientes consultas SQL en el **Query Tool** de pgAdmin:

*   **Verificar datos crudos**:
    ```sql
    -- Contar filas por año y mes en raw
    SELECT source_year, source_month, COUNT(*) AS total
    FROM raw.yellow_taxi_trips
    GROUP BY source_year, source_month
    ORDER BY source_year, source_month;
    ```
    
*   **Verificar tablas dimensionales**:
    ```sql
    SELECT * FROM clean.dim_vendor;
    SELECT * FROM clean.dim_payment_type;
    SELECT COUNT(*) FROM clean.dim_pickup_location;
    SELECT COUNT(*) FROM clean.dim_dropoff_location;
    ```

*   **Verificar tabla de hechos**:
    ```sql
    -- Conteo total
    SELECT COUNT(*) FROM clean.fact_trips;
    
    -- Distribución por mes
    SELECT source_year, source_month, COUNT(*) AS viajes
    FROM clean.fact_trips
    GROUP BY source_year, source_month
    ORDER BY source_year, source_month;
    
    -- Estadísticas generales
    SELECT
       ROUND(AVG(trip_distance)::numeric, 2)         AS distancia_promedio_millas,
       ROUND(AVG(total_amount)::numeric, 2)           AS tarifa_promedio_usd,
       ROUND(AVG(trip_duration_minutes)::numeric, 2)  AS duracion_promedio_min,
       MAX(passenger_count)                           AS max_pasajeros
    FROM clean.fact_trips;
    
    -- Consulta con joins a dimensiones
    SELECT
       v.vendor_name,
       p.payment_description,
       COUNT(*)                                       AS total_viajes,
       ROUND(AVG(f.total_amount)::numeric, 2)         AS tarifa_promedio
    FROM clean.fact_trips f
    JOIN clean.dim_vendor v ON f.vendor_key = v.vendor_key
    JOIN clean.dim_payment_type p ON f.payment_key = p.payment_key
    GROUP BY v.vendor_name, p.payment_description
    ORDER BY total_viajes DESC;
    ```

*   **Consultar métricas por proveedor**:
    ```sql
    SELECT v.vendor_name, SUM(f.total_amount) as total_ingresos
    FROM clean.fact_trips f
    JOIN clean.dim_vendor v ON f.vendor_key = v.vendor_key
    GROUP BY v.vendor_name;
    ```

---

## 🧠 Decisiones de Diseño

*   **Mage AI**: Elegido por su facilidad para desarrollar pipelines modulares y su integración nativa con Python y SQL, permitiendo ver previsualizaciones de datos en cada bloque.
*   **Arquitectura de Capas (Medallón)**:
    *   Separación de `raw` y `clean` para permitir el reprocesamiento de datos sin necesidad de volver a descargarlos desde la fuente externa.
*   **Modelo Dimensional (Estrella)**:
    *   Optimización de la tabla de hechos (`fact_trips`) para reducir redundancia de datos (usando IDs enteros en lugar de strings largos).
    *   Uso de índices en llaves foráneas y campos de fecha para acelerar las consultas analíticas.
*   **Sequences e Índices**: Se implementaron secuencias para las llaves primarias subrogadas y se crearon índices estratégicos para mejorar el rendimiento de los joins entre dimensiones y hechos.

---

## 📊 Descripción de Schemas, Tablas y Relaciones

### 📂 Schema: `raw`
Almacena los datos originales sin procesar.
*   **`yellow_taxi_trips`**: Contiene todas las columnas del conjunto de datos original (TLC), incluyendo timestamps, distancias, montos de tarifas, etc.

#### `raw.yellow_taxi_trips`
 
| Columna                | Tipo      | Descripción                          |
|------------------------|-----------|--------------------------------------|
| `vendorid`             | int8      | ID del proveedor del taxi            |
| `tpep_pickup_datetime` | timestamp | Fecha y hora de recogida             |
| `tpep_dropoff_datetime`| timestamp | Fecha y hora de entrega              |
| `passenger_count`      | float32   | Número de pasajeros                  |
| `trip_distance`        | float32   | Distancia del viaje (millas)         |
| `ratecodeid`           | float32   | ID del tipo de tarifa                |
| `pulocationid`         | float32   | ID de zona de recogida               |
| `dolocationid`         | float32   | ID de zona de entrega                |
| `payment_type`         | float32   | Tipo de pago                         |
| `fare_amount`          | float32   | Tarifa base                          |
| `tip_amount`           | float32   | Propina                              |
| `tolls_amount`         | float32   | Peajes                               |
| `total_amount`         | float32   | Monto total cobrado                  |
| `source_year`          | int16     | Año de origen del archivo fuente     |
| `source_month`         | int8      | Mes de origen del archivo fuente     |

### 📂 Schema: `clean` (Modelo Estrella)
Estructura optimizada para análisis.

#### Tablas de Dimensiones (`dim_*`)
*   **`dim_vendor`**: Información de los proveedores de servicios (Ej: VeriFone, Creative Mobile).
*   **`dim_payment_type`**: Catálogo de métodos de pago (Crédito, Efectivo, etc.).
*   **`dim_pickup_location`** / **`dim_dropoff_location`**: IDs de ubicación de origen y destino.

#### `clean.dim_vendor`
 
| Columna      | Tipo   | Descripción                      |
|--------------|--------|----------------------------------|
| `vendor_key` | SERIAL | Clave surrogate (PK)             |
| `vendor_id`  | INT    | ID original del proveedor (UK)   |
| `vendor_name`| TEXT   | Nombre del proveedor             |

#### `clean.dim_payment_type`
 
| Columna               | Tipo   | Descripción                          |
|-----------------------|--------|--------------------------------------|
| `payment_key`         | SERIAL | Clave surrogate (PK)                 |
| `payment_id`          | INT    | ID original del tipo de pago (UK)    |
| `payment_description` | TEXT   | Descripción del tipo de pago         |

#### `clean.dim_pickup_location`
 
| Columna      | Tipo   | Descripción              |
|--------------|--------|--------------------------|
| `pickup_key` | SERIAL | Clave surrogate (PK)     |
| `location_id`| INT    | ID de zona TLC (UK)      |

#### `clean.dim_dropoff_location`
 
| Columna       | Tipo   | Descripción              |
|---------------|--------|--------------------------|
| `dropoff_key` | SERIAL | Clave surrogate (PK)     |
| `location_id` | INT    | ID de zona TLC (UK)      |

#### Tabla de Hechos
*   **`fact_trips`**: La tabla principal que contiene las métricas cuantitativas.
    *   **Llaves**: `trip_key` (PK), `vendor_key` (FK), `payment_key` (FK), `pickup_key` (FK), `dropoff_key` (FK).
    *   **Métricas**: Montos (`fare`, `tip`, `total`), duración del viaje, distancia y recuento de pasajeros.

### 🔗 Relaciones
*   `fact_trips` (**N**) ──▶ (**1**) `dim_vendor`
*   `fact_trips` (**N**) ──▶ (**1**) `dim_payment_type`
*   `fact_trips` (**N**) ──▶ (**1**) `dim_pickup_location`
*   `fact_trips` (**N**) ──▶ (**1**) `dim_dropoff_location`

#### `clean.fact_trips`
 
| Columna                  | Tipo      | Descripción                              |
|--------------------------|-----------|------------------------------------------|
| `trip_key`               | BIGINT    | Clave surrogate del viaje (PK)           |
| `vendor_key`             | INT       | FK → `dim_vendor.vendor_key`             |
| `payment_key`            | INT       | FK → `dim_payment_type.payment_key`      |
| `pickup_key`             | INT       | FK → `dim_pickup_location.pickup_key`    |
| `dropoff_key`            | INT       | FK → `dim_dropoff_location.dropoff_key`  |
| `tpep_pickup_datetime`   | TIMESTAMP | Fecha y hora de inicio del viaje         |
| `tpep_dropoff_datetime`  | TIMESTAMP | Fecha y hora de fin del viaje            |
| `trip_duration_minutes`  | FLOAT     | Duración calculada del viaje en minutos  |
| `trip_distance`          | FLOAT     | Distancia recorrida (millas)             |
| `fare_amount`            | FLOAT     | Tarifa base                              |
| `tip_amount`             | FLOAT     | Propina                                  |
| `tolls_amount`           | FLOAT     | Peajes                                   |
| `total_amount`           | FLOAT     | Monto total cobrado                      |
| `passenger_count`        | INT       | Número de pasajeros                      |
| `source_year`            | INT       | Año de origen                            |
| `source_month`           | INT       | Mes de origen                            |

---

## 🔐 Manejo de Configuración Sensible
 
Toda configuración sensible se maneja mediante:
 
1. **Archivo `.env`** en la raíz del proyecto (no se sube a GitHub).
2. **Secrets de Mage AI** configurados en la interfaz de Mage (no se almacenan en el código fuente).
 
No se hardcodean credenciales en ningún bloque, script o archivo de configuración del repositorio.
