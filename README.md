# Trabajo Práctico Final — Introducción al Marketing Online y los Negocios Digitales

# Proyecto de Ecosistema de Datos (EcoBottle) - MKT Digital y Neg.Digitales

Este repositorio contiene el pipeline de ETL y la fuente de datos para el dashboard de KPIs de "EcoBottle", desarrollado como proyecto final.

## 📊 Dashboard de Resultados 

El dashboard interactivo con los KPIs finales se puede consultar en el siguiente enlace:

* **Enlace al Dashboard:** `[EN PROCESO]`

---

## 🚀 Guía Rápida de Ejecución
Sigue estos pasos para generar el Data Warehouse (`dw`) a partir de los datos `raw` localmente.

### 1. Configuración del Entorno
```bash
# 1. Clona este repositorio
git clone <URL_DE_TU_REPO>
cd <NOMBRE_DEL_REPO>

# 2. Crea y activa un entorno virtual
python -m venv .env
source .env/bin/activate  # En macOS/Linux
.\.env\Scripts\Activate   # En Windows PowerShell

# 3. Instala las dependencias requeridas
pip install -r requirements.txt

##4.Ejecutar el pipeline de transformación 
El script `Cambios.py` es la raíz del proyecto que orquesta todo el proceso.

```bash
python Cambios.py
 ```

**Verificar la salida:**
    Tras la ejecución, la carpeta `dw/` (Data Warehouse) deberá contener los 12 archivos `.CSV` transformados, listos para subir a Power BI.

Al finalizar, la carpeta `dw/` contendrá todos los archivos `.csv` limpios (`dim_store.csv`, `fact_payment.csv`,`fact_shipment.csv` etc.), listos para ser cargados en Power BI.

---


## 📈 Modelado de datos

Para modelar los datos y utilizarlos en este proyecto se presento un **esquema estrella** donde las tablas de hechos (`fact_`) se conectan con las dimensiones (`dim_`) para su posterior análisis.

### Diagramas de Estrella

A continuación se presentan los diagramas de estrella para cada tabla de hechos:
**Diagrama de `fact_nps_response`:**
!(./assets/fact_nps_response.png)
**Diagrama de `fact_payment`:**
!(./assets/fact_payment.png)
**Diagrama de `fact_sales_order_item`:**
!(./assets/fact_sales_order_item.png)
**Diagrama de `fact_sales_order`:**
!(./assets/fact_sales_order.png)
**Diagrama de `fact_shipment`:**
!(./assets/fact_shipment.png)
**Diagrama de `fact_web_sessions`:**
!(./assets/fact_web_sessions.png)



###Diccionario de datos
El Data Warehouse (`dw/`) esta formado por  6 Dimensiones y 6 Tablas de Hechos.

#### Dimensiones (`dw/`)

- **1. dim_calendar**
    **(Clave Primaria):** `id`
  - **Atributos:**
    - `id` 
    - `date` 
    - `day` 
    - `year` 
    - `day_name` 
    - `month_name` 
    - `quarter` 
    - `week_number` 
    - `year_month` 
    - `is_weekend` 

- **2. dim_customer**
  - **(Clave Primaria):** `id` 
  - **Atributos:**
    - `customer_key` - `email` - `first_name` - `last_name` - `phone` - `status` - `created_at` 

- **3. dim_product**

  - **(Clave Primaria):** `id` 
  - **Atributos:**
    - `product_key` - `sku` - `name` - `list_price` - `status` - `created_at` - `category_name` - -`parent_category_name` 

- **4. dim_address**

  - **(Clave Primaria):** `id` (Tipo: `INT`)
  - **Atributos:**
    - `address_key` - `line1` - `line2` - `city` - `province_name` - `province_code` - `postal_code` - `country_code` - `created_at` 

- **5. dim_store**

  - **(Clave Primaria):** `id` 
  - **Atributos:**
    - `store_key` - `name` - `line` - `city` - `province_name` - `province_code` - `postal_code` - `country_code` - `created_at` 

- **6. dim_channel**
  - **(Clave Primaria):** `id` 
  - **Atributos:**
    - `channel_key` - `code` - `name` 


#### Tablas de Hechos (`DW/`)

Las tablas de hechos contienen los indicadores y las claves foráneas que las conectan a las dimensiones.

- - **1. fact_sales_order**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `order_date_id` 
    - `customer_id` 
    - `channel_id` 
    - `store_id` 
    - `billing_address_id` 
    - `shipping_address_id`
  - **Atributos:**
    - `order_time`
    - `status_order` 
    - `currency_code` 
  - **Métricas:**
    - `subtotal`
    - `tax_amount`
    - `shipping_fee`
    - `total_amount`

- **2. fact_sales_order_item**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `order_id`
    - `order_date_id`  
    - `product_id`  
    - `customer_id`
    - `channel_id` 
    - `store_id` 
  - **Métricas:**
    - `quantity`
    - `unit_price`
    - `discount_amount`
    - `line_total`

- **3. fact_payment**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `paid_at_date_id` 
    - `customer_id` 
    - `billing_address_id` 
    - `channel_id` 
    - `store_id` 
  - **Atributos:**
    - `method` 
    - `status_payment` 
    - `paid_at_time` 
    - `transaction_ref` 
  - **Métricas:**
    - `amount`

- **4. fact_shipment**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `shipped_at_date_id` 
    - `delivered_at_date_id` 
    - `customer_id` 
    - `shipping_address_id` 
    - `channel_id` 
  - **Atributos:**
    - `carrier` 
    - `tracking_number` 
    - `shipped_at_time` 
    - `delivered_at_time` 
  - **Métricas:**
    - `dias_de_entrega` 

- **5. fact_web_session**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `started_at_date_id` 
    - `ended_at_date_id` 
    - `customer_id` 
  - **Atributos:**
    - `started_at_time` 
    - `ended_at_time` 
    - `source` 
    - `device` 

- **6. fact_nps_response**
  - **(PK):** `id`
  - **FK (Claves Foráneas):**
    - `responded_at_date_id` 
    - `customer_id`
    - `channel_id` 
  - **Atributos:**
    - `responded_at_time` 
  - **Métricas:**
    - `score` 






# AUTOR:
**Bruno Carrara**💡





**Consigna y documento principal:** [Trabajo Práctico Final](https://docs.google.com/document/d/15RNP3FVqLjO4jzh80AAkK6mUR5DOLqPxLjQxqvdzrYg/edit?usp=sharing)
**Diagrama Entidad Relación:** [DER](./assets/DER.png)