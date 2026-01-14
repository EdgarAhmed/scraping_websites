#!/usr/bin/env python3
"""
Script de scraping para ebooks de MediaMarkt con actualización en Google Drive
EXACTLY matches the old notebook scraping logic
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import time
import os
import math
import re
import sys
import io
import json
import hashlib  # Importar hashlib para generar IDs

# ============================================ #
#                                              #
#       CONFIGURACIÓN DE MARCAS DE EBOOKS      #
#                                              #
# ============================================ #

marcas_ebooks = [
    'amazon', 'kindle', 'kobo', 'pocketbook', 'bq', 'tolino', 'onyx boox',
    'remarkable', 'sony', 'reader', 'nook', 'barnes noble', 'bookeen',
    'energy sistem', 'wolder', 'dingoo', 'artect', 'trekstor', 'iriver',
    'aluratek', 'emporia', 'hanvon', 'pandigital', 'velocity micro',
    'copia', 'foxit', 'ectaco', 'entourage', 'icarus', 'geniatech',
    'pocketbook', 'inkbook', 'fidibook', 'mediapress', 'vivitar',
    'supersonic', 'visual land', 'digma', 'texet', 'prestigio', 'ritmix',
    'odeon', 'maxvi', 'teclast', 'chuwi', 'cube', 'onda', 'aigo', 'newsmy',
    'wexler', 'ebw', 'bens', 'mustek', 'philips', 'lenovo', 'asus',
    'dell', 'hp', 'acer', 'samsung', 'lg', 'microsoft', 'apple'
]

def extraer_marca_ebook(nombre):
    """
    Función para extraer la marca del ebook del nombre
    """
    if pd.isna(nombre):
        return 'Desconocido'

    nombre_lower = str(nombre).lower()

    # Casos especiales que necesitan manejo específico
    if 'kindle' in nombre_lower:
        return 'Amazon'
    if 'kobo' in nombre_lower:
        return 'Kobo'
    if 'pocketbook' in nombre_lower:
        return 'PocketBook'
    if 'tolino' in nombre_lower:
        return 'Tolino'
    if 'onyx boox' in nombre_lower:
        return 'Onyx Boox'
    if 'remarkable' in nombre_lower:
        return 'ReMarkable'
    if 'nook' in nombre_lower or 'barnes noble' in nombre_lower:
        return 'Barnes & Noble'
    if 'bookeen' in nombre_lower:
        return 'Bookeen'
    if 'energy sistem' in nombre_lower:
        return 'Energy Sistem'
    if 'inkbook' in nombre_lower:
        return 'Inkbook'
    if 'fidibook' in nombre_lower:
        return 'Fidibook'

    # Buscar coincidencias exactas de marcas
    for marca in marcas_ebooks:
        # Buscar la marca como palabra completa para evitar falsos positivos
        if f' {marca} ' in f' {nombre_lower} ' or nombre_lower.startswith(marca + ' '):
            # Manejar nombres que deben ser capitalizados correctamente
            if marca in ['bq', 'kobo']:
                return marca.upper()
            elif marca == 'kindle':
                return 'Amazon'
            elif marca == 'pocketbook':
                return 'PocketBook'
            elif marca == 'tolino':
                return 'Tolino'
            elif marca == 'onyx boox':
                return 'Onyx Boox'
            elif marca == 'remarkable':
                return 'ReMarkable'
            elif marca in ['nook', 'barnes noble']:
                return 'Barnes & Noble'
            elif marca == 'bookeen':
                return 'Bookeen'
            elif marca == 'energy sistem':
                return 'Energy Sistem'
            elif marca == 'inkbook':
                return 'Inkbook'
            elif marca == 'fidibook':
                return 'Fidibook'
            elif marca == 'bq':
                return 'BQ'
            else:
                return marca.title()  # Devuelve con la primera letra mayúscula

    return 'Otra marca'
    
# ============================================ #
#                                              #
#    FUNCIONES PARA GENERAR IDs ÚNICOS         #
#                                              #
# ============================================ #

def generar_id_consistente(nombre):
    """
    Genera un ID único y consistente basado en el nombre del producto
    El mismo producto siempre tendrá el mismo ID
    """
    # Normalizar el nombre: minúsculas, sin espacios extra, caracteres especiales
    nombre_normalizado = str(nombre).lower().strip()
    nombre_normalizado = re.sub(r'\s+', ' ', nombre_normalizado)  # Reemplazar múltiples espacios por uno
    
    # Crear un hash MD5 del nombre normalizado
    hash_obj = hashlib.md5(nombre_normalizado.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()
    
    # Tomar los primeros 12 caracteres del hash para un ID legible
    return hash_hex[:12]

def generar_id_descriptivo(nombre, marca=""):
    """
    Genera un ID más descriptivo combinando marca y hash
    """
    # Normalizar inputs
    nombre_norm = str(nombre).lower().strip()
    marca_norm = str(marca).lower().strip() if marca else ""
    
    # Crear una clave combinada
    if marca_norm:
        clave = f"{marca_norm}:{nombre_norm}"
    else:
        clave = nombre_norm
    
    # Generar hash
    hash_obj = hashlib.md5(clave.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()[:8]  # Más corto
    
    # Si tenemos marca, crear ID del tipo "MARCA_HASH"
    if marca_norm:
        marca_abrev = marca_norm[:4].upper()
        return f"{marca_abrev}_{hash_hex}"
    else:
        return hash_hex

# ============================================ #
#                                              #
#          LIMPIA PRECIOS                      #
#                                              #
# ============================================ #

def limpiar_columnas_precio(df):
    """
    Limpia las columnas precio_actual y precio_original para extraer valores numéricos
    """
    print("\n" + "="*60)
    print("LIMPIANDO COLUMNAS DE PRECIO")
    print("="*60)
    
    try:
        # Guardar copias de los precios originales antes de limpiar
        if 'precio_actual_original' not in df.columns:
            df['precio_actual_original'] = df['precio_actual'].copy()
        if 'precio_original_original' not in df.columns:
            df['precio_original_original'] = df['precio_original'].copy()
        
        # Estadísticas antes de limpiar
        print(f"📊 Total de registros: {len(df)}")
        print(f"💰 Precios actuales únicos antes de limpiar: {df['precio_actual'].nunique()}")
        print(f"📋 Precios originales únicos antes de limpiar: {df['precio_original'].nunique()}")
        
        # Función auxiliar para limpiar un precio
        def limpiar_precio_individual(precio_str):
            if pd.isna(precio_str):
                return None
            precio_str = str(precio_str)
            # Eliminar todo excepto números y comas
            precio_str = re.sub(r'[^\d,]', '', precio_str)
            # Convertir comas a puntos
            precio_str = precio_str.replace(',', '.')
            try:
                return float(precio_str)
            except:
                return None
        
        # Limpiar ambas columnas de precio
        df['precio_actual'] = df['precio_actual'].apply(limpiar_precio_individual)
        df['precio_original'] = df['precio_original'].apply(limpiar_precio_individual)
        
        # Calcular descuento si ambos precios están disponibles
        df['descuento_porcentaje'] = None
        mask = df['precio_actual'].notna() & df['precio_original'].notna() & (df['precio_original'] > 0)
        df.loc[mask, 'descuento_porcentaje'] = (
            (df.loc[mask, 'precio_original'] - df.loc[mask, 'precio_actual']) / 
            df.loc[mask, 'precio_original'] * 100
        )
        
        # Estadísticas después de limpiar
        print(f"✅ Columnas de precio limpiadas exitosamente")
        print(f"\n💰 PRECIO ACTUAL:")
        print(f"   - Valores únicos: {df['precio_actual'].nunique()}")
        print(f"   - Valores nulos: {df['precio_actual'].isna().sum()}")
        print(f"   - Rango: {df['precio_actual'].min():.2f}€ - {df['precio_actual'].max():.2f}€")
        print(f"   - Promedio: {df['precio_actual'].mean():.2f}€")
        
        print(f"\n📋 PRECIO ORIGINAL:")
        print(f"   - Valores únicos: {df['precio_original'].nunique()}")
        print(f"   - Valores nulos: {df['precio_original'].isna().sum()}")
        print(f"   - Rango: {df['precio_original'].min():.2f}€ - {df['precio_original'].max():.2f}€")
        print(f"   - Promedio: {df['precio_original'].mean():.2f}€")
        
        # Estadísticas de descuentos
        productos_con_descuento = df['descuento_porcentaje'].notna().sum()
        if productos_con_descuento > 0:
            print(f"\n🎯 PRODUCTOS CON DESCUENTO: {productos_con_descuento}")
            print(f"   - Descuento promedio: {df['descuento_porcentaje'].mean():.1f}%")
            print(f"   - Descuento máximo: {df['descuento_porcentaje'].max():.1f}%")
            print(f"   - Descuento mínimo: {df['descuento_porcentaje'].min():.1f}%")
        
        # Mostrar primeros valores
        print("\n📋 Primeros 5 valores de precios limpios:")
        muestra = df[['precio_actual_original', 'precio_actual', 
                      'precio_original_original', 'precio_original',
                      'descuento_porcentaje']].head()
        print(muestra.to_string())
        
        return df
        
    except Exception as e:
        print(f"❌ Error limpiando columnas de precio: {e}")
        import traceback
        traceback.print_exc()
        return df

# ============================================ #
#                                              #
#       CONFIGURACIÓN GOOGLE DRIVE             #
#                                              #
# ============================================ #

def configurar_google_drive():
    """
    Configura y autentica con Google Drive usando credenciales de servicio
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        # Verificar si hay credenciales disponibles
        credenciales_json = (
            os.environ.get('GOOGLE_CREDENTIALS_JSON_2')
            or os.environ.get('GOOGLE_CREDENTIALS_JSON'))
        
        if not credenciales_json:
            print("⚠️  No se encontraron credenciales de Google Drive en variables de entorno")
            return None
        
        # Crear credenciales desde JSON string
        creds_dict = json.loads(credenciales_json)
        scopes = ['https://www.googleapis.com/auth/drive']
        
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=scopes
        )
        
        # Crear servicio de Google Drive
        service = build('drive', 'v3', credentials=credentials)
        
        print("✅ Google Drive configurado exitosamente")
        return service
        
    except ImportError:
        print("❌ Módulos de Google API no instalados. Instala: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
        return None
    except Exception as e:
        print(f"❌ Error configurando Google Drive: {e}")
        return None

def buscar_archivo_drive(service, nombre_archivo, folder_id):
    """
    Busca un archivo en Google Drive
    """
    try:
        query = f"name = '{nombre_archivo}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name, modifiedTime)"
        ).execute()
        
        files = results.get('files', [])
        return files[0] if files else None
        
    except Exception as e:
        print(f"❌ Error buscando archivo en Drive: {e}")
        return None

def descargar_archivo_drive(service, file_id):
    """
    Descarga un archivo de Google Drive.
    Soporta CSV reales y Google Sheets (exportándolos).
    """
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import io

        # Obtener metadata para saber el tipo de archivo
        metadata = service.files().get(
            fileId=file_id,
            fields="mimeType"
        ).execute()

        mime_type = metadata.get("mimeType")

        fh = io.BytesIO()

        # 🟢 CASO 1: Google Sheets → EXPORT
        if mime_type == "application/vnd.google-apps.spreadsheet":
            print("📄 Archivo es Google Sheets, exportando como CSV")
            request = service.files().export(
                fileId=file_id,
                mimeType="text/csv"
            )

        # 🟢 CASO 2: Archivo binario (CSV real)
        else:
            print("📄 Archivo es binario, descargando directamente")
            request = service.files().get_media(fileId=file_id)

        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        contenido_bytes = fh.getvalue()

        try:
            return contenido_bytes.decode("utf-8")
        except UnicodeDecodeError:
            print("⚠️ UTF-8 falló, intentando latin-1")
            return contenido_bytes.decode("latin-1")

    except Exception as e:
        print(f"❌ Error descargando archivo de Drive: {e}")
        return None

def subir_archivo_drive(service, nombre_archivo, contenido_csv, folder_id, file_id=None):
    """
    Sube un archivo CSV a Google Drive.
    Si file_id se proporciona, actualiza el archivo existente.
    Si no, crea un nuevo archivo.
    """
    try:
        from googleapiclient.http import MediaIoBaseUpload
        import io
        
        # Crear un objeto de bytes del CSV
        csv_bytes = contenido_csv.encode('utf-8')
        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes), 
            mimetype='text/csv',
            resumable=False
        )
        
        # Si hay un file_id, actualizar el archivo existente
        if file_id:
            print(f"📤 Actualizando archivo existente en Drive (ID: {file_id})")
            file = service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
            print("✅ Archivo actualizado en Drive")
        # Si no, crear un nuevo archivo
        else:
            print("📤 Creando nuevo archivo en Drive")
            file_metadata = {
                'name': nombre_archivo,
                'parents': [folder_id],
                'mimeType': 'text/csv'
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"✅ Nuevo archivo creado en Drive (ID: {file.get('id')})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error subiendo archivo a Drive: {e}")
        import traceback
        traceback.print_exc()
        return False

# Folder de Google Drive donde se guarda el histórico
def actualizar_csv_drive(
    df_nuevo,
    folder_id="1cSW4uOfw4x61a-R6TAOyn6ejEHNiyX0v",
    nombre_archivo="ebooks_mediamarkt.csv"
):
    import io
    import pandas as pd

    def leer_csv_seguro(contenido):
        # Eliminar bytes NUL que rompen pandas
        contenido = contenido.replace('\x00', '')
        try:
            return pd.read_csv(
                io.StringIO(contenido),
                sep=None,
                engine="python",
                on_bad_lines="skip"
            )
        except Exception:
            print("⚠️ CSV histórico corrupto, se ignora y se recrea")
            return None

    print("\n" + "="*60)
    print("ACTUALIZANDO GOOGLE DRIVE – HISTÓRICO REAL (APPEND)")
    print("="*60)

    service = configurar_google_drive()
    if not service:
        print("⚠️ Google Drive no disponible")
        return False

    archivo_existente = buscar_archivo_drive(service, nombre_archivo, folder_id)

    if archivo_existente:
        print("📁 Archivo histórico encontrado")

        contenido = descargar_archivo_drive(service, archivo_existente["id"])
        if not contenido:
            print("❌ No se pudo descargar el histórico")
            return False

        df_existente = leer_csv_seguro(contenido)

        if df_existente is None or df_existente.empty:
            print("🆕 Histórico inválido → usando solo datos nuevos")
            df_combinado = df_nuevo.copy()
        else:
            print(f"📊 Filas históricas: {len(df_existente)}")
            df_combinado = pd.concat(
                [df_existente, df_nuevo],
                ignore_index=True,
                sort=False
            )
    else:
        print("🆕 No existe histórico, creando nuevo")
        df_combinado = df_nuevo.copy()

    filas_antes = len(df_combinado)
    df_combinado = df_combinado.drop_duplicates()
    filas_despues = len(df_combinado)

    print(f"🧹 Duplicados exactos eliminados: {filas_antes - filas_despues}")
    print(f"📊 Total final en histórico: {len(df_combinado)}")

    csv_contenido = df_combinado.to_csv(index=False, encoding="utf-8")

    # CORRECCIÓN APLICADA: Siempre usar el ID del archivo existente para actualizarlo
    if archivo_existente:
        # Usar el ID del archivo existente para ACTUALIZARLO (no crear uno nuevo)
        subir_exitoso = subir_archivo_drive(
            service,
            nombre_archivo,
            csv_contenido,
            folder_id,
            archivo_existente["id"]  # Esto hace que se actualice el mismo archivo
        )
    else:
        # Si no existe, crear uno nuevo
        subir_exitoso = subir_archivo_drive(
            service,
            nombre_archivo,
            csv_contenido,
            folder_id,
            None  # Sin ID para crear nuevo archivo
        )

    if subir_exitoso:
        print("✅ Histórico actualizado correctamente en Google Drive")
        return True
    else:
        print("❌ Error subiendo el archivo a Drive")
        return False

# ============================================ #
#                                              #
#      FUNCIONES DEL SCRAPING                  #
#                                              #
# ============================================ #

def setup_chrome_options():
    """Configura Chrome para ejecución headless (optimizado)"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    return chrome_options

def mediamark_mob_(url):
    """Inicializa el navegador Chrome - EXACTLY like old notebook"""
    try:
        chrome_options = setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        driver.get(url)
        time.sleep(2)

        # Aceptar cookies
        try:
            aceptar = driver.find_element(By.ID, "pwa-consent-layer-accept-all-button")
            aceptar.click()
            print("Cookies aceptadas")
        except Exception as e:
            print(f"Error aceptando cookies: {e}")

        time.sleep(3)
        
        return driver
        
    except Exception as e:
        print(f"❌ Error inicializando Chrome: {e}")
        raise

def obtener_total_articulos(driver):
    """
    Obtiene el número total de artículos del span y calculas las páginas necesarias
    EXACTLY like old notebook
    """
    try:
        elemento_total = driver.find_element(By.CSS_SELECTOR, 'span.sc-94eb08bc-0.AKpzk')
        texto_total = elemento_total.text
        
        numero_total = re.search(r'\((\d+)', texto_total)
        
        if numero_total:
            total_articulos = int(numero_total.group(1))
            print(f"📊 Total de artículos encontrados: {total_articulos}")
            
            productos_por_pagina = 12
            total_paginas = math.ceil(total_articulos / productos_por_pagina)
            print(f"📄 Total de páginas a recorrer: {total_paginas}")
            
            return total_articulos, total_paginas
        else:
            print("❌ No se pudo extraer el número total de artículos")
            return None, 10
    
    except Exception as e:
        print(f"❌ Error obteniendo el total de artículos: {e}")
        return None, 10

# ============================================ #
#                                              #
#      extraer productos                       #
#                                              #
# ============================================ #

def extraer_precio_producto(contenedor_producto):
    """
    Función específica para extraer tanto el precio actual como el precio original
    EXACTLY like old notebook - MODIFICADO para extraer ambos precios
    """
    try:
        precio_actual = None
        precio_original = None
        
        # 1. INTENTAR OBTENER PRECIO ACTUAL (rebajado)
        # Selector para precio actual rebajado
        try:
            precio_actual_elem = contenedor_producto.find_element(
                By.CSS_SELECTOR, 
                'span.sc-94eb08bc-0.dYbTef.sc-a69e154d-2.dJKnju'
            )
            precio_actual = precio_actual_elem.text
            print(f"      ✅ Precio actual encontrado: {precio_actual}")
        except:
            pass
        
        # 2. INTENTAR OBTENER PRECIO ORIGINAL (antes del descuento)
        # Selector para precio original (tachado)
        try:
            precio_original_elem = contenedor_producto.find_element(
                By.CSS_SELECTOR, 
                'span.sc-94eb08bc-0.iJxYPS'
            )
            precio_original = precio_original_elem.text
            print(f"      📋 Precio original encontrado: {precio_original}")
        except:
            pass
        
        # 3. Si no se encontró precio actual con el selector específico,
        # intentar con otros selectores (para productos sin descuento)
        if not precio_actual:
            try:
                precio_final = contenedor_producto.find_element(
                    By.CSS_SELECTOR, 
                    'span.sc-94eb08bc-0.dYbTef.sc-8a3a8cd8-2.csCDkt'
                )
                precio_actual = precio_final.text
                print(f"      ✅ Precio actual (alternativo) encontrado: {precio_actual}")
            except:
                pass
        
        if not precio_actual:
            try:
                precio_normal = contenedor_producto.find_element(
                    By.CSS_SELECTOR, 
                    'span.sc-94eb08bc-0.OhHlB.sc-8a3a8cd8-2.csCDkt'
                )
                precio_actual = precio_normal.text
                print(f"      ✅ Precio actual (normal) encontrado: {precio_actual}")
            except:
                pass
        
        # 4. Si aún no hay precios, buscar cualquier elemento que contenga '€'
        if not precio_actual or not precio_original:
            try:
                elementos_precio = contenedor_producto.find_elements(
                    By.XPATH, 
                    ".//*[contains(text(), '€')]"
                )
                precios_encontrados = []
                for elem in elementos_precio:
                    texto = elem.text.strip()
                    if '€' in texto and any(c.isdigit() for c in texto):
                        precios_encontrados.append(texto)
                
                # Si encontramos al menos un precio
                if precios_encontrados:
                    if len(precios_encontrados) >= 2:
                        # Generalmente el primero es el actual y el segundo el original
                        precio_actual = precio_actual or precios_encontrados[0]
                        precio_original = precio_original or precios_encontrados[1]
                    else:
                        precio_actual = precio_actual or precios_encontrados[0]
                        precio_original = precio_original or precio_actual
                        
                    print(f"      🔍 Precios encontrados por búsqueda: {precios_encontrados}")
                    
            except Exception as e:
                print(f"      ⚠️  Error en búsqueda alternativa de precios: {e}")
        
        # 5. Si no se encontró precio original pero sí precio actual,
        # asumir que son iguales (producto sin descuento)
        if precio_actual and not precio_original:
            precio_original = precio_actual
            print(f"      📝 Precio original igual al actual (sin descuento)")
        
        # 6. Si no se encontró ningún precio
        if not precio_actual:
            precio_actual = "Precio no disponible"
            precio_original = "Precio no disponible"
            print(f"      ⚠️  No se encontró precio")
        
        return {
            'precio_actual': precio_actual,
            'precio_original': precio_original
        }
        
    except Exception as e:
        print(f"      ❌ Error extrayendo precios: {e}")
        return {
            'precio_actual': f"Error: {e}",
            'precio_original': f"Error: {e}"
        }


def extraer_link_producto(contenedor_producto, driver, profundidad=0, max_profundidad=3):
    """
    Extrae el enlace del producto usando múltiples estrategias con esperas.
    Si no encuentra enlace en el contenedor actual, sube recursivamente en el DOM.
    """
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    if profundidad > max_profundidad:
        return "No disponible"
    
    # Estrategia 0: Esperar a que el contenedor esté presente y visible
    try:
        WebDriverWait(driver, 2).until(
            lambda d: contenedor_producto.is_displayed()
        )
    except Exception:
        pass  # Continuar incluso si la espera falla
    
    # Estrategia 1: Buscar un enlace específico para el producto (selector exacto)
    try:
        enlace_element = WebDriverWait(contenedor_producto, 2).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-test="mms-router-link-product-list-item-link_mp"]'))
        )
        href = enlace_element.get_attribute('href')
        if href and 'mediamarkt' in href:
            resultado = href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
            print(f"      ✅ Enlace encontrado (Estrategia 1 - Selector exacto)")
            return resultado
    except Exception:
        pass
    
    # Estrategia 2: Lista de selectores CSS alternativos (más genéricos)
    posibles_selectores = [
        ('a[href*="/p/"]', "Href /p/"),
        ('a[href*="/product/"]', "Href /product/"),
        ('a[data-test*="product-list-item-link"]', "data-test genérico"),
        ('a[data-test*="product-link"]', "data-test product-link"),
        ('a.sc-8a3a8cd8-2', "Clase específica del enlace"),
        ('a[class*="product-link"]', "Clase que contiene product-link"),
        ('a', "Cualquier enlace"),
    ]
    
    for selector, descripcion in posibles_selectores:
        try:
            elementos = contenedor_producto.find_elements(By.CSS_SELECTOR, selector)
            for elemento in elementos:
                href = elemento.get_attribute("href")
                if not href:
                    continue
                
                # Filtrar enlaces que no sean de productos o sean de tracking
                if "mediamarkt" not in href:
                    continue
                
                # Verificar patrones típicos de enlaces de producto
                if "/p/" not in href and "/product/" not in href:
                    continue
                
                # Convertir a URL absoluta si es necesario
                if not href.startswith("http"):
                    href = "https://www.mediamarkt.es" + href
                
                print(f"      ✅ Enlace encontrado (Estrategia 2 - {descripcion})")
                return href
        except Exception:
            continue
    
    # Estrategia 3: Buscar en elementos que contengan el texto del título del producto
    try:
        # Primero obtener el nombre del producto si está disponible
        nombre_element = contenedor_producto.find_elements(By.CSS_SELECTOR, 'p[data-test="product-title"]')
        if nombre_element:
            nombre_producto = nombre_element[0].text
            # Buscar enlaces que contengan palabras clave del nombre del producto
            enlaces = contenedor_producto.find_elements(By.TAG_NAME, "a")
            for enlace in enlaces:
                href = enlace.get_attribute('href')
                if href and 'mediamarkt' in href and any(keyword in href.lower() for keyword in ['p-', 'product-', '/p/', '/product/']):
                    resultado = href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
                    print(f"      ✅ Enlace encontrado (Estrategia 3 - Por título del producto)")
                    return resultado
    except Exception:
        pass
    
    # Estrategia 4: Subir recursivamente en el DOM (como último recurso)
    if profundidad < max_profundidad:
        try:
            padre = contenedor_producto.find_element(By.XPATH, "./..")
            print(f"      🔍 Subiendo al elemento padre (profundidad: {profundidad+1})")
            return extraer_link_producto(padre, driver, profundidad+1, max_profundidad)
        except Exception as e:
            print(f"      ⚠️  No se pudo subir al elemento padre: {e}")
    
    # Estrategia 5: Verificar si el contenedor ES un enlace
    try:
        tag_name = contenedor_producto.tag_name.lower()
        if tag_name == 'a':
            href = contenedor_producto.get_attribute('href')
            if href and 'mediamarkt' in href:
                resultado = href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
                print(f"      ✅ Enlace encontrado (Estrategia 5 - El contenedor es un enlace)")
                return resultado
    except Exception:
        pass
    
    # Si ninguna estrategia funcionó
    print(f"      ⚠️  No se pudo extraer enlace (profundidad: {profundidad})")
    return "No disponible"

def extraer_productos_pagina(driver):
    """
    Extrae los productos de una sola página
    EXACTAMENTE igual que el código original - MODIFICADO para ambos precios
    """
    productos_pagina = []

    try:
        titulos = driver.find_elements(By.CSS_SELECTOR, 'p[data-test="product-title"]')
        print(f"   🔍 Encontrados {len(titulos)} productos en la página")

        for i, titulo in enumerate(titulos, start=1):
            try:
                nombre = titulo.text.strip()

                # 🔗 ENLACE (ancestro <a>)
                try:
                    enlace_elem = titulo.find_element(By.XPATH, ".//ancestor::a[1]")
                    enlace = enlace_elem.get_attribute("href")
                    if enlace and not enlace.startswith("http"):
                        enlace = "https://www.mediamarkt.es" + enlace
                except Exception:
                    enlace = "No disponible"

                # 🧱 CONTENEDOR — MISMA LÓGICA QUE EL NOTEBOOK
                contenedor = titulo
                for _ in range(5):
                    contenedor = contenedor.find_element(By.XPATH, "./..")
                    precios = contenedor.find_elements(By.XPATH, ".//*[contains(text(), '€')]")
                    if precios:
                        break

                # 💰 PRECIOS (ACTUAL Y ORIGINAL)
                precios_dict = extraer_precio_producto(contenedor)
                precio_actual = precios_dict['precio_actual']
                precio_original = precios_dict['precio_original']

                # 🏷️ MARCA
                marca = extraer_marca_ebook(nombre)

                # 🆔 ID CONSISTENTE
                producto_id = generar_id_consistente(nombre)

                productos_pagina.append({
                    'id': producto_id,
                    'nombre': nombre,
                    'precio_actual': precio_actual,
                    'precio_original': precio_original,
                    'marca': marca,
                    'enlace': enlace
                })

            except Exception as e:
                print(f"   ❌ Error en producto {i}: {e}")
                continue

        return productos_pagina

    except Exception as e:
        print(f"❌ Error extrayendo productos de la página: {e}")
        return productos_pagina
    
def extraer_productos(driver):
    """
    Extrae todos los productos EXACTLY like old notebook
    """
    productos_data = []
    contador_global = 1
    
    try:
        total_articulos, total_paginas = obtener_total_articulos(driver)
        
        print(f"🔄 Total de artículos: {total_articulos}")
        print(f"📄 Páginas calculadas: {total_paginas}")
        
        criterios_ordenacion = [
            "currentprice+desc",
            "currentprice+asc",
            "relevance",
            "name+asc",
            "name+desc"
        ]
        
        productos_unicos = set()
        
        for criterio in criterios_ordenacion:
            print(f"🎯 Usando criterio de ordenación: {criterio}")
            
            for pagina in range(1, 31):
                try:
                    print(f"📖 Página {pagina}/30 - Criterio: {criterio}")
                    
                    url_pagina = f"https://www.mediamarkt.es/es/category/ebooks-249.html?sort={criterio}&page={pagina}"
                    
                    driver.get(url_pagina)
                    time.sleep(2)
                    
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'p[data-test="product-title"]'))
                        )
                    except:
                        print(f"❌ La página {pagina} no cargó correctamente, pasando a siguiente criterio")
                        break
                    
                    productos_pagina = extraer_productos_pagina(driver)
                    
                    for producto in productos_pagina:
                        nombre_producto = producto['nombre']
                        if nombre_producto not in productos_unicos:
                            productos_unicos.add(nombre_producto)
                            producto['numero'] = contador_global
                            contador_global += 1
                            productos_data.append(producto)
                    
                    print(f"✅ Página {pagina}: {len(productos_pagina)} productos, Total únicos: {len(productos_data)}")
                    
                    if len(productos_pagina) < 12:
                        print("📝 Última página detectada")
                        break
                    
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Error en página {pagina}: {e}")
                    continue
        
        print(f"\n📊 Resumen final: {len(productos_data)} productos únicos de {len(criterios_ordenacion)} criterios")
        
        if total_articulos:
            porcentaje = (len(productos_data) / total_articulos) * 100
            print(f"📈 Se extrajo el {porcentaje:.1f}% del total de artículos")
        
        return productos_data
                
    except Exception as e:
        print(f"❌ Error extrayendo productos: {e}")
        return productos_data

def guardar_en_dataframe(productos_data):
    """
    Convierte la lista de productos en un DataFrame y lo guarda en CSV
    EXACTLY like old notebook - MODIFICADO para ambos precios
    """
    if not productos_data:
        print("No hay datos para guardar")
        return None
    
    df = pd.DataFrame(productos_data)
    
    fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['fecha_extraccion'] = fecha_extraccion
    
    # Verificar si ya tenemos IDs generados
    if 'id' not in df.columns:
        print("\n" + "="*60)
        print("GENERANDO IDs ÚNICOS PARA PRODUCTOS")
        print("="*60)
        # Si no hay columna 'id', generamos los IDs
        df['id'] = df['nombre'].apply(generar_id_consistente)
        print(f"✅ IDs generados para {len(df)} productos")
        print(f"📊 IDs únicos: {df['id'].nunique()}")
    
    # Limpiar columnas de precio
    df = limpiar_columnas_precio(df)
    
    # Orden de columnas con ambos precios incluidos
    column_order = [
        'fecha_extraccion', 'id', 'numero', 'nombre', 'marca', 
        'precio_actual', 'precio_original', 'descuento_porcentaje', 'enlace'
    ]
    
    # Añadir columnas originales si existen
    if 'precio_actual_original' in df.columns:
        column_order.append('precio_actual_original')
    if 'precio_original_original' in df.columns:
        column_order.append('precio_original_original')
    
    # Asegurar que todas las columnas existan
    existing_columns = [col for col in column_order if col in df.columns]
    missing_columns = [col for col in column_order if col not in df.columns]
    
    if missing_columns:
        print(f"⚠️  Advertencia: Columnas faltantes en DataFrame: {missing_columns}")
    
    df = df[existing_columns]
    
    # Crear directorio si no existe
    os.makedirs("scraping_results", exist_ok=True)
    
    # Guardar archivo local
    nombre_archivo = f"scraping_results/ebooks_mediamarkt_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = nombre_archivo
    df.to_csv(file_path, index=False, encoding='utf-8')
    
    print(f"\n✅ Datos guardados en: {file_path}")
    print(f"📊 Total de productos únicos: {len(df)}")
    
    # Estadísticas mejoradas
    print(f"\n📈 ESTADÍSTICAS DETALLADAS:")
    print(f"🏷️  Marcas diferentes: {df['marca'].nunique()}")
    
    # Distribución de precios actuales
    print(f"\n💰 DISTRIBUCIÓN DE PRECIOS ACTUALES:")
    print(f"   - Precio promedio: {df['precio_actual'].mean():.2f}€")
    print(f"   - Precio mediano: {df['precio_actual'].median():.2f}€")
    print(f"   - Precio mínimo: {df['precio_actual'].min():.2f}€")
    print(f"   - Precio máximo: {df['precio_actual'].max():.2f}€")
    
    # Productos con descuento
    productos_con_descuento = df['descuento_porcentaje'].notna().sum()
    print(f"\n🎯 PRODUCTOS CON DESCUENTO: {productos_con_descuento} ({productos_con_descuento/len(df)*100:.1f}%)")
    if productos_con_descuento > 0:
        descuento_promedio = df['descuento_porcentaje'].mean()
        print(f"   - Descuento promedio: {descuento_promedio:.1f}%")
        
        # Top 5 productos con mayor descuento
        print(f"\n🏆 TOP 5 PRODUCTOS CON MAYOR DESCUENTO:")
        top_descuentos = df.dropna(subset=['descuento_porcentaje']).nlargest(5, 'descuento_porcentaje')
        for idx, row in top_descuentos.iterrows():
            print(f"   {row['nombre'][:40]}...")
            print(f"     Precio original: {row['precio_original']:.2f}€")
            print(f"     Precio actual: {row['precio_actual']:.2f}€")
            print(f"     Descuento: {row['descuento_porcentaje']:.1f}%")
            print()
    
    # Estadísticas de enlaces
    enlaces_validos = df[df['enlace'] != 'No disponible']['enlace'].count()
    print(f"\n🔗 Enlaces extraídos: {enlaces_validos} de {len(df)} productos")
    
    return df, file_path

# ============================================ #
#                                              #
#      FUNCION PRINCIPAL                       #
#                                              #
# ============================================ #

def main():
    """Función principal"""
    print("="*60)
    print("SCRAPING DE EBOOKS - MEDIAMARKT")
    print("="*60)
    print("📌 AHORA CON PRECIO ACTUAL Y PRECIO ORIGINAL")
    print(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    driver = None
    
    try:
        url = "https://www.mediamarkt.es/es/category/ebooks-249.html?sort=currentprice+desc"
        
        print(f"\n🌐 Accediendo a: {url}")
        
        driver = mediamark_mob_(url)
        
        productos_data = extraer_productos(driver)
        
        if not productos_data:
            print("No se extrajeron productos")
            return False
        
        df, archivo_csv = guardar_en_dataframe(productos_data)
        
        if df is None:
            print("❌ Error creando DataFrame. Terminando ejecución.")
            return False
        
        print("\n🔄 Actualizando Google Drive (APPEND mode)...")
        
        drive_actualizado = actualizar_csv_drive(df)
        
        if drive_actualizado:
            print("✅ Google Drive actualizado exitosamente (APPEND)")
        else:
            print("⚠️  No se pudo actualizar Google Drive (puede ser falta de credenciales)")
        
        print("\n" + "="*60)
        print("RESUMEN EJECUCIÓN")
        print("="*60)
        print(f"✅ Scraping completado exitosamente")
        print(f"📦 Productos obtenidos hoy: {len(df)}")
        print(f"🔑 IDs únicos generados: {df['id'].nunique()}")
        print(f"🏷️  Marcas diferentes encontradas: {df['marca'].nunique()}")
        print(f"💰 Precios actuales válidos: {df['precio_actual'].notna().sum()}")
        print(f"📋 Precios originales válidos: {df['precio_original'].notna().sum()}")
        print(f"🎯 Productos con descuento: {df['descuento_porcentaje'].notna().sum()}")
        print(f"🔗 Enlaces extraídos: {df[df['enlace'] != 'No disponible']['enlace'].count()}")
        print(f"📁 Archivo local generado: {archivo_csv}")
        print(f"💾 Google Drive: Datos añadidos al archivo histórico")
        
        return True
            
    except Exception as e:
        print(f"Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if driver:
            try:
                driver.quit()
                print("\n🛑 Navegador cerrado")
            except:
                pass
        
        print("\n" + "="*60)
        print("EJECUCIÓN FINALIZADA")
        print("="*60)
