import time
from etl.extract import extract_all_data
from etl.transform import transform_all_data
from etl.load import load_to_csv


def main():
    """
    Orquesta el proceso ETL completo:
    1. Extrae datos de /RAW
    2. Transforma los datos en un esquema estrella
    3. Carga los datos transformados en /DW
    """
    print("==  Iniciando ETL para EcoBottle   ==")
    
    start_time = time.time()
    
    try:
        # --- EXTRACCIÓN (E) ---
        print("[E] Iniciando Extracción..")
        raw_data = extract_all_data()
        
        if raw_data is None:
            print("Error en la extracción.")
            return
            
        print("[E] Extracción completada.\n")
        
        # --- TRANSFORMACIÓN (T) ---
        print("[T] Iniciando Transformación...")
        dw_tables = transform_all_data(raw_data)
        
        if dw_tables is None:
            print("Error en la transformación.")
            return
            
        print("[T] Transformación completada.\n")
        
        # --- CARGA (L) ---
        print("[L] Iniciando Carga en /DW...")
        
        for table_name, df in dw_tables.items():
            filename = f"{table_name}.csv"
            print(f"  -> Guardando tabla: {filename}...")
            load_to_csv(df, filename)
            
        print("[L] Carga de datos completada.\n")
        
        # --- Finalización ---
        
        print(f"  Proceso  completado ")
        print("=======================")
        print(f"Se generaron {len(dw_tables)} tablas en la carpeta /DW.")

    except Exception as e:
        print(f"\n¡ERROR INESPERADO")
        print(f"Detalle: {e}")

if __name__ == "__main__":
    main()