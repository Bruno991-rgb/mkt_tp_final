
import pandas as pd


def _get_date_id(date_series, dim_calendar):

    dates = pd.to_datetime(date_series, errors='coerce').dt.normalize()
    date_map = dim_calendar.set_index('date')['id']
    return dates.map(date_map)

def _get_time(date_series):

    times = pd.to_datetime(date_series, errors='coerce')
    return times.dt.strftime('%H:%M:%S').fillna('00:00:00')

def _add_surrogate_key_and_reorder(df, sort_by_col, nk_col_name, sk_col_name='id'):
    
    df_sorted = df.sort_values(by=sort_by_col)
    df_sorted.reset_index(drop=True, inplace=True)
    df_sorted[sk_col_name] = df_sorted.index + 1
    
    cols = [sk_col_name, nk_col_name]
    cols += [col for col in df_sorted.columns if col not in cols]
    
    return df_sorted[cols]


def create_dim_calendar(data):
    print("  -> Creando dim_calendar...")
    
    # 1. Recolectar todas las fechas
    date_sources = [
        (data.get('sales_order'), 'order_date'),
        (data.get('web_session'), 'started_at'),
        (data.get('nps_response'), 'responded_at'),
        (data.get('payment'), 'paid_at'),
        (data.get('shipment'), 'shipped_at'),
        (data.get('shipment'), 'delivered_at'),
        (data.get('customer'), 'created_at'),
        (data.get('address'), 'created_at'),
        (data.get('product'), 'created_at'),
    ]
    
    all_dates_list = []
    for df, col in date_sources:
        if df is not None and col in df:
            all_dates_list.append(pd.to_datetime(df[col], errors='coerce'))
        
    all_dates = pd.concat(all_dates_list).dropna()
    all_dates_normalized = all_dates.dt.normalize()

    if all_dates_normalized.empty:
        print("Advertencia: No se encontraron fechas válidas para dim_calendar.")
        return pd.DataFrame(columns=['id', 'date', 'day', 'month', 'year', 'day_name', 'month_name', 'quarter', 'week_number', 'year_month', 'is_weekend'])

    # 2. Determinar rango dinámico
    min_date = all_dates_normalized.min()
    max_date = all_dates_normalized.max()
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    df_calendar = pd.DataFrame(date_range, columns=['date'])
    
    # 3. Enriquecer
    df_calendar['day'] = df_calendar['date'].dt.day
    df_calendar['month'] = df_calendar['date'].dt.month
    df_calendar['year'] = df_calendar['date'].dt.year
    df_calendar['day_name'] = df_calendar['date'].dt.day_name()
    df_calendar['month_name'] = df_calendar['date'].dt.month_name()
    df_calendar['quarter'] = df_calendar['date'].dt.quarter
    df_calendar['week_number'] = df_calendar['date'].dt.isocalendar().week.astype(int)
    df_calendar['year_month'] = df_calendar['date'].dt.strftime('%Y-%m')
    df_calendar['is_weekend'] = df_calendar['day_name'].isin(['Saturday', 'Sunday'])
    
    # 4. Crear SK
    df_calendar.reset_index(drop=True, inplace=True)
    df_calendar['id'] = df_calendar.index + 1
    
    # 5. Ordenar
    cols = ['id'] + [col for col in df_calendar if col != 'id']
    df_calendar = df_calendar[cols]
    
    return df_calendar

def create_dim_customer(data):
    """
    Crea la Dimensión de Cliente (dim_customer).
    """
    print("  -> Creando dim_customer...")
    df = data['customer'].copy()
    df = df.rename(columns={'customer_id': 'customer_key'})
    cols = ['customer_key', 'email', 'first_name', 'last_name', 'phone', 'status', 'created_at']
    df = df[cols]
    return _add_surrogate_key_and_reorder(df, 'customer_key', 'customer_key')

def create_dim_channel(data):
    """
    Crea la Dimensión de Canal (dim_channel).
    """
    print("  -> Creando dim_channel...")
    df = data['channel'].copy()
    df = df.rename(columns={'channel_id': 'channel_key'})
    df = _add_surrogate_key_and_reorder(df, 'channel_key', 'channel_key')
    return df[['id', 'channel_key', 'code', 'name']]

def create_dim_address(data):
    """
    Crea la Dimensión de Dirección (dim_address).
    """
    print("  -> Creando dim_address...")
    address = data['address'].copy()
    province = data['province'].copy()
    
    df = pd.merge(address, province, on='province_id', how='left')
    
    df = df.rename(columns={
        'address_id': 'address_key',
        'name': 'province_name',
        'code': 'province_code'
    })
    
    cols = ['address_key', 'line1', 'line2', 'city', 'province_name', 
             'province_code', 'postal_code', 'country_code', 'created_at']
    df = df[cols]
    return _add_surrogate_key_and_reorder(df, 'address_key', 'address_key')

def create_dim_product(data):
    """
    Crea la Dimensión de Producto (dim_product).
    """
    print("  -> Creando dim_product...")
    product = data['product'].copy()
    category = data['product_category'].copy()

    product['category_id'] = product['category_id'].astype(str)
    category['category_id'] = category['category_id'].astype(str)
    category['parent_id'] = category['parent_id'].astype(str)
    
    parent_cats = category[['category_id', 'name']].rename(
        columns={'category_id': 'parent_id', 'name': 'parent_category_name'}
    )
    
    categories_enriched = pd.merge(
        category, parent_cats, on='parent_id', how='left',
        suffixes=('_cat', '_parent')
    )

    df = pd.merge(
        product, categories_enriched, on='category_id', how='left',
        suffixes=('_prod', '') 
    )
    
    df = df.rename(columns={
        'product_id': 'product_key',
        'name_prod': 'name',
        'name': 'category_name'
    })
    
    cols = ['product_key', 'sku', 'name', 'list_price', 'status', 
             'created_at', 'category_name', 'parent_category_name']
    df = df[cols]
    df['category_name'] = df['category_name'].fillna('Sin Categoría')
    df['parent_category_name'] = df['parent_category_name'].fillna('Sin Categoría')

    return _add_surrogate_key_and_reorder(df, 'product_key', 'product_key')

def create_dim_store(data):
    """
    Crea la Dimensión de Tienda (dim_store).
    """
    print("  -> Creando dim_store...")
    store = data['store'].copy()
    address = data['address'].copy()
    province = data['province'].copy()
    
    store_addr = pd.merge(store, address, on='address_id', how='left')
    df = pd.merge(store_addr, province, on='province_id', how='left')
    
    df = df.rename(columns={
        'store_id': 'store_key',
        'name_x': 'name',
        'line1': 'line',
        'name_y': 'province_name',
        'code': 'province_code'
    })
    
    cols = ['store_key', 'name', 'line', 'city', 'province_name', 
             'province_code', 'postal_code', 'country_code', 'created_at']
    df = df[cols]

    return _add_surrogate_key_and_reorder(df, 'store_key', 'store_key')

# --- Funciones de Creación de Hechos ---

def create_fact_sales_order(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Órdenes de Venta (fact_sales_order).
    """
    print("  -> Creando fact_sales_order...")
    df = data['sales_order'].copy()
        
    df = df.rename(columns={'order_id': 'id', 'status': 'status_order'})
    df['order_date_id'] = _get_date_id(df['order_date'], dim_calendar)
    df['order_time'] = _get_time(df['order_date'])
    
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['billing_address_id'] = df['billing_address_id'].fillna(-1).astype(int)
    df['shipping_address_id'] = df['shipping_address_id'].fillna(-1).astype(int)

    cols = [
        'id', 'customer_id', 'channel_id', 'store_id', 'order_date_id', 'order_time',
        'billing_address_id', 'shipping_address_id', 'status_order', 'currency_code',
        'subtotal', 'tax_amount', 'shipping_fee', 'total_amount'
    ]
    return df[cols]

def create_fact_sales_order_item(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Items de Venta (fact_sales_order_item).
    """
    print("  -> Creando fact_sales_order_item...")
    items = data['sales_order_item'].copy()
    orders = data['sales_order'][['order_id', 'customer_id', 'channel_id', 'store_id', 'order_date']]
    
    df = pd.merge(items, orders, on='order_id', how='left')
    df = df.rename(columns={'order_item_id': 'id'})
    df['order_date_id'] = _get_date_id(df['order_date'], dim_calendar)
    
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['product_id'] = df['product_id'].fillna(-1).astype(int)

    cols = [
        'id', 'order_id', 'customer_id', 'channel_id', 'store_id', 'product_id', 'order_date_id',
        'quantity', 'unit_price', 'discount_amount', 'line_total'
    ]
    return df[cols]

def create_fact_payment(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Pagos (fact_payment).
    """
    print("  -> Creando fact_payment...")
    payments = data['payment'].copy()
    orders = data['sales_order'][['order_id', 'customer_id', 'billing_address_id', 'channel_id', 'store_id']]
    
    df = pd.merge(payments, orders, on='order_id', how='left')
    df = df.rename(columns={'payment_id': 'id', 'status': 'status_payment'})
    df['paid_at_date_id'] = _get_date_id(df['paid_at'], dim_calendar)
    df['paid_at_time'] = _get_time(df['paid_at'])
    
    df['store_id'] = df['store_id'].fillna(-1).astype(int)
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['billing_address_id'] = df['billing_address_id'].fillna(-1).astype(int)

    cols = [
        'id', 'customer_id', 'billing_address_id', 'channel_id', 'store_id',
        'method', 'status_payment', 'amount', 'paid_at_date_id', 'paid_at_time',
        'transaction_ref'
    ]
    return df[cols]

def create_fact_shipment(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Envíos (fact_shipment).
    """
    print("  -> Creando fact_shipment...")
    shipments = data['shipment'].copy()
    orders = data['sales_order'][['order_id', 'customer_id', 'shipping_address_id', 'channel_id']]
    
    df = pd.merge(shipments, orders, on='order_id', how='left')
    df = df.rename(columns={'shipment_id': 'id'})
    
    df['shipped_at'] = pd.to_datetime(df['shipped_at'], errors='coerce')
    df['delivered_at'] = pd.to_datetime(df['delivered_at'], errors='coerce')

    df['shipped_at_date_id'] = _get_date_id(df['shipped_at'], dim_calendar)
    df['delivered_at_date_id'] = _get_date_id(df['delivered_at'], dim_calendar)
    df['shipped_at_time'] = _get_time(df['shipped_at'])
    df['delivered_at_time'] = _get_time(df['delivered_at'])

    df['dias_de_entrega'] = (df['delivered_at'] - df['shipped_at']).dt.days

    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)
    df['shipping_address_id'] = df['shipping_address_id'].fillna(-1).astype(int)

    cols = [
        'id', 'customer_id', 'shipping_address_id', 'channel_id', 'carrier',
        'shipped_at_date_id', 'shipped_at_time',
        'delivered_at_date_id', 'delivered_at_time', 'tracking_number', 
        'dias_de_entrega'
    ]
    return df[cols]

def create_fact_web_session(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Sesiones Web (fact_web_session).
    """
    print("  -> Creando fact_web_session...")
    df = data['web_session'].copy()
    
    df = df.rename(columns={'session_id': 'id'})
    df['started_at_date_id'] = _get_date_id(df['started_at'], dim_calendar)
    df['ended_at_date_id'] = _get_date_id(df['ended_at'], dim_calendar)
    df['started_at_time'] = _get_time(df['started_at'])
    df['ended_at_time'] = _get_time(df['ended_at'])
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)

    cols = [
        'id', 'customer_id', 'started_at_date_id', 'started_at_time',
        'ended_at_date_id', 'ended_at_time', 'source', 'device'
    ]
    return df[cols]

def create_fact_nps_response(data, dim_calendar):
    """
    Crea la Tabla de Hechos de Respuestas NPS (fact_nps_response).
    """
    print("  -> Creando fact_nps_response...")
    df = data['nps_response'].copy()
    
    df = df.rename(columns={'nps_id': 'id'})
    df['responded_at_date_id'] = _get_date_id(df['responded_at'], dim_calendar)
    df['responded_at_time'] = _get_time(df['responded_at'])
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    df['channel_id'] = df['channel_id'].fillna(-1).astype(int)

    cols = [
        'id', 'customer_id', 'channel_id', 'responded_at_date_id',
        'responded_at_time', 'score'
    ]
    return df[cols]

# --- Función Orquestadora (ACTUALIZADA) ---

def transform_all_data(data):
    """
    Orquesta todas las transformaciones y devuelve un diccionario
    con los DataFrames del Data Warehouse (dim_ y fact_).
    """
    if data is None:
        print("Error: No hay datos (data es None) para transformar.")
        return None
        
    print("Iniciando proceso de transformación (T)...")
    
    dw_tables = {}
    
    try:
        # --- Paso 1: Crear Dimensiones ---
        print("Procesando Dimensiones...")
        
        dw_tables['dim_calendar'] = create_dim_calendar(data)
        dw_tables['dim_customer'] = create_dim_customer(data)
        dw_tables['dim_product'] = create_dim_product(data)
        dw_tables['dim_channel'] = create_dim_channel(data)
        dw_tables['dim_address'] = create_dim_address(data)
        dw_tables['dim_store'] = create_dim_store(data)
        
        # --- Paso 2: Crear Hechos ---
        print("Procesando Hechos...")
        dim_calendar = dw_tables['dim_calendar'] 
        
        if dim_calendar.empty:
            print("Advertencia: dim_calendar está vacía. Las FKs de fecha en las tablas de hechos pueden ser nulas.")
        
        dw_tables['fact_sales_order'] = create_fact_sales_order(data, dim_calendar)
        dw_tables['fact_sales_order_item'] = create_fact_sales_order_item(data, dim_calendar)
        dw_tables['fact_payment'] = create_fact_payment(data, dim_calendar)
        dw_tables['fact_shipment'] = create_fact_shipment(data, dim_calendar)
        dw_tables['fact_web_session'] = create_fact_web_session(data, dim_calendar)
        dw_tables['fact_nps_response'] = create_fact_nps_response(data, dim_calendar)
        
        print("Proceso de transformación completado.\n")
        return dw_tables
        
    except Exception as e:
        print(f"Error inesperado durante la transformación: {e}")
        return None

if __name__ == '__main__':
    print("Advertencia: Este módulo (transform.py) contiene las funciones de transformación.")
    print("Advertencia: No está diseñado para ejecutarse directamente.")
    print("Advertencia: Impórtalo y llama a 'transform_all_data(data)' desde tu script principal (ej: main.py).")