# Arquitectura del Sistema

El proyecto implementa una solución de datos end-to-end utilizando una arquitectura de microservicios orquestada con **Docker Compose**. El objetivo es garantizar un entorno reproducible, escalable y aislado para el procesamiento de datos de NYC Taxi.

## Representación Textual de la Arquitectura

```text
                                [ Internet: NYC TLC Parquet Files ]
                                              |
                                              | (Extract)
                                              v
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Docker Compose                                                                          │
│                                                                                         │
│   ┌────────────────────┐          ┌───────────────────────────────────┐                 │
│   │      Mage AI       │          │           PostgreSQL              │                 │
│   │   (Orquestador)    │          │        (Data Warehouse)           │                 │
│   │                    │          │                                   │                 │
│   │  [ Pipeline 1 ]    │ (ETL)    │  ┌─────────────────────────────┐  │   ┌──────────┐  │
│   │  (Raw Ingestion)  ───────────>│  │       Esquema RAW           │  │   │ pgAdmin  │  │
│   │                    │    ┌────────│ (Datos Crudos Inmutables)   │  │   │   (UI)   │  │
│   │                    │    │     │  └──────────────┬──────────────┘  │   │          │  │
│   │                    │    │     │                 |                 │   │          │  │
│   │                    │    │     │                 |                 │<──┤          │  │
│   │  [ Pipeline 2 ]   <─────┘     │  ┌──────────────v──────────────┐  │   │          │  │
│   │ (Clean Transf.)   ───────────>│  │       Esquema CLEAN         │  │   └──────────┘  │
│   │                    │   (ELT)  │  │    (Modelo Dimensional)     │  │                 │
│   └────────────────────┘          │  └─────────────────────────────┘  │                 │
│                                   └───────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```


## Servicios Principales
- **Mage AI**: Motor de orquestación de datos que permite definir pipelines como código Python/SQL.
- **PostgreSQL**: Base de datos relacional utilizada como almacén de datos (Data Warehouse), segmentada en capas.
- **pgAdmin**: Interfaz gráfica para la gestión y consulta de la base de datos PostgreSQL.

## Evidencia de Infraestructura (Docker)

### Imágenes del Proyecto
![Docker Images](../screenshots/docker%20images.png)
*Figura: Listado de imágenes descargadas y construidas para el entorno.*

### Volúmenes de Persistencia
![Docker Volumes](../screenshots/docker%20volumes.png)
*Figura: Volúmenes configurados para mantener la persistencia de datos y configuraciones.*

### Ejecución de Contenedores
![Docker Run](../screenshots/docker%20run.png)
*Figura: Registro de inicio de los servicios mediante Docker Compose.*

## Flujo de Datos
1. **Extracción**: Mage AI descarga archivos Parquet desde el servidor de NYC TLC.
2. **Carga (Raw)**: Los datos se cargan sin alteraciones significativas en el esquema `raw`.
3. **Transformación (Clean)**: Un segundo pipeline lee de `raw`, aplica reglas de limpieza y puebla el modelo dimensional en el esquema `clean`.
4. **Visualización**: Los resultados finales son consultables mediante pgAdmin.
