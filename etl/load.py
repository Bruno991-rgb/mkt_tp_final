import pandas as pd
from pathlib import Path

from .extract import DW_DIR

def load_to_csv(df, filename):
    """
    Guarda un DataFrame en un archivo .csv dentro del directorio DW.
    FORZANDO el punto (.) como separador decimal.
    """
    try:
        # Asegurarse de que el directorio DW exista (método de pathlib)
        DW_DIR.mkdir(parents=True, exist_ok=True)
        
        file_path = DW_DIR / filename 
    
        df.to_csv(
            file_path, 
            index=False, 
            decimal='.',
            encoding='utf-8' 
        ) 
        
    except Exception as e:
        print(f"Error al guardar el archivo {filename}: {e}")

if __name__ == '__main__':
    print("Iniciando prueba de carga...")
    test_df = pd.DataFrame({'col1': [1.5, 2.0], 'col2': ['A', 'B']})
    load_to_csv(test_df, 'test_table.csv')
    print("Prueba de carga finalizada.")
    print(f"Revisa {DW_DIR / 'test_table.csv'} y confirma que usa '.' (punto) decimal.")