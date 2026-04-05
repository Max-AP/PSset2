# 🚖 NY Taxi Data Pipeline

Pipeline de ingesta y transformación de datos de viajes en taxi amarillo de Nueva York, orquestado con **Mage AI**, almacenado en **PostgreSQL** y visualizable mediante **pgAdmin**.

---

## 📌 Objetivo del Proyecto

Construir un pipeline de datos end-to-end que:

1. Descarga datos históricos de viajes en taxi amarillo de NYC desde el portal oficial TLC.
2. Ingesta los datos crudos en un schema `raw` de PostgreSQL.
3. Transforma y modela los datos en un schema `clean` usando un modelo dimensional (estrella).
4. Expone los datos listos para análisis y consultas analíticas.

---

## 🏗️ Arquitectura

┌─────────────────────────────────────────────────────────┐
│ docker-compose                                          │
│                                                         │
│ ┌──────────────┐   ┌────────────────┐   ┌──────────┐    │
│ │ Mage AI      │   │ PostgreSQL     │   │ pgAdmin  │    │
│ │ (orquestador)│──▶│(data-warehouse)│──▶│ (UI)     │    │
│ │ :6789        │   │ :5432          │   │ :9000    │    │
│ └──────────────┘   └────────────────┘   └──────────┘    │
└─────────────────────────────────────────────────────────┘

| Servicio         | Imagen              | Puerto  | Descripción                               |
|------------------|---------------------|---------|-----------------------------------------  |
| `data-warehouse` | `postgres:13`       | `5432`  | Base de datos PostgreSQL (data warehouse) |
| `orquestador`    | `mageai/mageai`     | `6789`  | Orquestador de pipelines (Mage AI)        |
| `warehouse-ui`   | `dpage/pgadmin4`    | `9000`  | Interfaz web para administrar PostgreSQL  |

---

## ⚙️ Pasos para Levantar el Entorno

### Pre-requisitos

- [Docker](https://www.docker.com/) y [Docker Compose](https://docs.docker.com/compose/) instalados.
- Git instalado.

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd PSset2
```
