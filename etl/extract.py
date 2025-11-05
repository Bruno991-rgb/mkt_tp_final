import pandas as pd
from pathlib import Path 

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'RAW'
DW_DIR = BASE_DIR / 'DW'

TABLE_NAMES = [
    'address', 'channel', 'customer', 'nps_response', 'payment',
    'product', 'product_category', 'province', 'sales_order',
    'sales_order_item', 'shipment', 'store', 'web_session'
]

def extract_all_data(data_dir=DATA_DIR):
    """
    Carga todas las tablas .csv desde el directorio RAW a un diccionario
    de DataFrames de pandas.
    """
    data = {}
    print(f"Iniciando extracción de datos desde: {data_dir}")
    
    try:
        for table in TABLE_NAMES:
            file_path = data_dir / f"{table}.csv" 
            
            if not file_path.exists():
                 print(f"No se encontró el archivo: {file_path}")
                 raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
                 
            data[table] = pd.read_csv(file_path)
            print(f"  -> Tabla '{table}' cargada exitosamente.")
            
        print("Extracción de datos completada.\n")
        return data
    
    except FileNotFoundError as e:
        print(f"Fallo (Archivo no encontrado): {e}")
        return None
    except Exception as e:
        print(f"Error inesperado durante la extracción: {e}")
        return None

if __name__ == '__main__':
    print("Iniciando prueba de extracción...")
    raw_data = extract_all_data()
    if raw_data:
        print("\nPrueba de extracción exitosa.")
        print(f"Tablas cargadas: {list(raw_data.keys())}")
        print(f"\nPrimeras 5 filas de 'customer':")
        print(raw_data['customer'].head())