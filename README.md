# Prueba Tenica Ingeniero de datos

## Olist E-Commerce ETL

Este proyecto está diseñado para realizar un proceso ETL (Extract, Transform, Load) sobre el dataset Brazilian E-Commerce Public Dataset by Olist. Los datos son extraídos de tres archivos CSV, transformados y cargados en una base de datos MySQL.

### Tabla de Contenidos

- [Introducción](#introducción)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)
- [Descripción del Proyecto](#descripción-del-proyecto)
- [Estructura de Archivos](#estructura-de-archivos)
- [Instalación](#instalación)
- [Ejecutar el Proyecto](#ejecutar-el-proyecto)
- [Consultas SQL](#consultas-sql)

## Introducción

Este proyecto tiene como objetivo cargar datos relacionados con órdenes, pagos y reseñas de un dataset de Olist en una base de datos MySQL, utilizando un proceso ETL en Python.

El flujo ETL realizado incluye los siguientes pasos:

**1. Extracción**: Los archivos CSV `olist_orders_dataset.csv`, `olist_order_payments_dataset.csv` y `olist_order_reviews_dataset.csv` son leídos desde un directorio local.

**2. Transformación**: Se limpian los datos, convirtiendo fechas a formato `DATETIME` y se gestionan los valores nulos.

**3. Carga**: Los datos transformados son cargados en las tablas correspondientes de la base de datos MySQL.

## Tecnologías Utilizadas

- **Python**: Lenguaje de programación utilizado para desarrollar el proceso ETL.
- **Pandas**: Librería para la manipulación y análisis de datos.
- **SQLAlchemy**: ORM utilizado para interactuar con la base de datos MySQL.
- **MySQL**: Sistema de gestión de bases de datos.
- **dotenv**: Para la gestión de variables de entorno.
- **PyMySQL**: Conector para interactuar con MySQL desde Python.

## Descripción del Proyecto

Este proyecto se basa en tres datasets proporcionados por Olist:

**1. olist_orders_dataset**: Contiene la información sobre las órdenes realizadas por los clientes.

**2. olist_order_payments_dataset**: Registra los pagos asociados a las órdenes.

**3. olist_order_reviews_dataset**: Contiene las reseñas de los clientes sobre los productos comprados.

Los datos extraídos de estos archivos CSV son transformados y cargados en una base de datos MySQL para su posterior análisis.

## Estructura de Archivos

```bash
/olist-ecommerce-etl
├── /data
│ ├── olist_orders_dataset.csv
│ ├── olist_order_payments_dataset.csv
│ ├── olist_order_reviews_dataset.csv
├── etl_olist.py # Script principal para ejecutar el proceso ETL
└── README.md # Este archivo
```

## Instalación

Para ejecutar este proyecto en tu máquina local, sigue estos pasos:

**1.** Clona el repositorio:

```bash
git clone https://github.com/JhonBoyaca/Prueba-Tenica-Ingeniero-de-datos.git
```

**2.** Navega al directorio del proyecto:

```bash
cd Prueba-Tenica-Ingeniero-de-datos
```

**3.** Crea un entorno virtual:

```bash
python -m venv venv
```

**4.** Activa el entorno virtual:

- En Windows:

```bash
venv\Scripts\activate
```

**5.** Instala las dependencias:

```bash
pip install pandas SQLAlchemy pymysql python-dotenv
```

**6.** Crea un archivo .env con las credenciales de tu base de datos MySQL:

```text
DB_USER=root
DB_PASS=tu_contraseña
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=olist_db
```

## Ejecutar el Proyecto

Una vez que hayas configurado el archivo `.env` y las dependencias, puedes ejecutar el script ETL con el siguiente comando:

```bash
python etl_olist.py
```

Este comando leerá los archivos CSV, transformará los datos y los cargará en la base de datos MySQL especificada.

## Consultas SQL

A continuación se presentan algunas consultas SQL que pueden ser útiles para analizar los datos en la base de datos:

**1.** Distribución de órdenes por estado:

```sql
SELECT order_status, COUNT(\*) AS status_count
FROM olist_orders_dataset
GROUP BY order_status;
```

**2.** Top 3 de métodos de pago más utilizados:

```sql
SELECT payment_type, COUNT(\*) AS payment_count
FROM olist_order_payments_dataset
GROUP BY payment_type
ORDER BY payment_count DESC
LIMIT 3;
```

**3.** Top 3 de scores de las reseñas de los clientes:

```sql
SELECT review_score, COUNT(\*) AS orders_count
FROM olist_order_reviews_dataset
GROUP BY review_score
ORDER BY orders_count DESC
LIMIT 3;
```

**4.** Órdenes con mayor cantidad de pagos:

```sql
SELECT order_id, COUNT(\*) AS payment_count
FROM olist_order_payments_dataset
GROUP BY order_id
ORDER BY payment_count DESC
LIMIT 3;
```

**5.** Número de comentarios con mensajes vacíos:

```sql
SELECT COUNT(\*) AS empty_reviews_count
FROM olist_order_reviews_dataset
WHERE review_comment_message IS NULL;
```

**6.** Promedio de las puntuaciones de las reseñas:

```sql
SELECT AVG(review_score) AS average_review_score
FROM olist_order_reviews_dataset;
```
