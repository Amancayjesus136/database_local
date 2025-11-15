# upload_to_sheets.py
# -*- coding: utf-8 -*-
"""
Script para automatizar la carga de múltiples archivos CSV o Parquet en múltiples Google Sheets,
reemplazando solo si hay cambios.
Permite indicar rutas a archivos individuales o carpetas; si es carpeta, concatena todos los archivos CSV/Parquet dentro.
"""
import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Configuración global ===
# Arreglo de hojas de cálculo y sus pestañas/hijos
SPREADSHEETS = [
    {
        # URL o ID de la hoja de cálculo
        "url": "https://docs.google.com/spreadsheets/d/1GDENDvDbSRJdFninD43X73xKykH_lHundkTRtpow55g",
        # Lista de pestañas y rutas (pueden ser a archivo o carpeta)
        "sheets": [
            {"sheet_name": "data_prueba", "path": "/Users/amancayjesus/Documents/projects/python/database_local/input/pmt_ofi.csv"}
            # {"sheet_name": "clientes", "path": "/home/jesus/Documents/pmol/dev/python.csv"},
            # {"sheet_name": "process",  "path": "/home/jesus/Documents/vscode/show/pyspark/DATA-SC-DET08-process-2025-05-01/"},
            # {"sheet_name": "ventas", "path": "/ruta/a/archivo.parquet"},
        ]
    },
    # Agrega más hojas de cálculo si lo necesitas
]

# Ruta al JSON de credenciales de Service Account
auth_path = "/Users/amancayjesus/Documents/projects/python/database_local/auth/credentials.json"

# === Funciones ===

def authenticate(creds_path):
    """
    Autentica con Service Account y retorna cliente gspread.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)


def read_path_to_df(path):
    """
    Lee un archivo CSV o Parquet, o todos los archivos dentro de una carpeta.
    Soporta múltiples encodings al leer CSV.
    Retorna un DataFrame concatenado y con NaN reemplazados por cadena vacía.
    """

    encodings_to_try = ["utf-8", "latin-1", "iso-8859-1", "cp1252"]
    dfs = []

    # Función interna para leer un CSV con fallback de encoding
    def read_csv_with_encoding(file_path):
        for enc in encodings_to_try:
            try:
                print(f"  -> Leyendo archivo '{file_path}' con encoding: {enc}")
                return pd.read_csv(file_path, dtype=str, encoding=enc)
            except UnicodeDecodeError:
                print(f"     Falló con encoding {enc}, probando siguiente...")
        raise UnicodeDecodeError(f"No se pudo leer '{file_path}' con ningún encoding válido.")

    # Si es carpeta, leer todos los archivos
    if os.path.isdir(path):
        for fname in sorted(os.listdir(path)):
            full = os.path.join(path, fname)
            ext = os.path.splitext(fname)[1].lower()

            if ext == ".csv":
                try:
                    dfs.append(read_csv_with_encoding(full))
                except Exception as e:
                    print(f"  [ERROR] No se pudo leer: {full} — {e}")

            elif ext == ".parquet":
                try:
                    print(f"  -> Leyendo Parquet: {full}")
                    dfs.append(pd.read_parquet(full))
                except Exception as e:
                    print(f"  [ERROR] No se pudo leer Parquet: {full} — {e}")

    # Si es archivo individual
    else:
        if not os.path.exists(path):
            print(f"Ruta no encontrada: {path}")
            return pd.DataFrame()

        ext = os.path.splitext(path)[1].lower()

        if ext == ".csv":
            dfs.append(read_csv_with_encoding(path))
        elif ext == ".parquet":
            print(f"  -> Leyendo Parquet: {path}")
            dfs.append(pd.read_parquet(path))
        else:
            print(f"Extensión no soportada: {ext}")
            return pd.DataFrame()

    if not dfs:
        print("No se encontró ningún archivo válido para procesar.")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    return df.fillna("")

def fetch_sheet_as_df(spreadsheet, sheet_name):
    """
    Obtiene el contenido de la pestaña como DataFrame, o None si no existe.
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return None
    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df.fillna("")


def replace_sheet(spreadsheet, sheet_name, df):
    """
    Reemplaza o crea la pestaña con el DataFrame dado.
    """
    try:
        ws = spreadsheet.worksheet(sheet_name)
        spreadsheet.del_worksheet(ws)
    except gspread.exceptions.WorksheetNotFound:
        pass
    ws = spreadsheet.add_worksheet(
        title=sheet_name,
        rows=str(len(df) + 1),
        cols=str(len(df.columns))
    )
    ws.insert_row(df.columns.tolist(), index=1)
    rows = df.values.tolist()
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        block = rows[i : i + batch_size]
        ws.insert_rows(block, row=i + 2)


def process_all():
    """
    Itera en SPREADSHEETS, procesa cada ruta (archivo o carpeta) y actualiza pestañas solo si cambia.
    """
    client = authenticate(auth_path)

    for book in SPREADSHEETS:
        print(f"\nProcesando Spreadsheet: {book['url']}")
        try:
            spreadsheet = client.open_by_url(book['url'])
        except Exception as e:
            print(f"  Error al abrir la hoja: {e}")
            continue

        for entry in book['sheets']:
            name = entry['sheet_name']
            path = entry['path']
            print(f"  -> Pestaña '{name}' desde '{path}'")
            if not os.path.exists(path):
                print(f"     Ruta no encontrada: {path}")
                continue

            df_local = read_path_to_df(path)
            df_sheet = fetch_sheet_as_df(spreadsheet, name)
            if df_sheet is not None and df_sheet.equals(df_local):
                print(f"     Sin cambios en '{name}', se omite.")
            else:
                print(f"     Cambios detectados o inexistente -> reemplazando '{name}'...")
                replace_sheet(spreadsheet, name, df_local)
                print(f"     '{name}' actualizado.")


if __name__ == "__main__":
    process_all()
