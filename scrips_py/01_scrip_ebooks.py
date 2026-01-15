#!/usr/bin/env python3
"""
Script de scraping para ebooks de MediaMarkt con actualización en Google Drive
MODIFICADO: Incluye precio original y precio rebajado
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
import hashlib

# ============================================ #
#          CONFIGURACIÓN DE MARCAS             #
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

def extraer_marca(nombre):
    """Función para extraer la marca del ebook del nombre"""
    if pd.isna(nombre):
        return 'Desconocido'

    nombre_lower = str(nombre).lower()

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

    for marca in marcas_ebooks:
        if f' {marca} ' in f' {nombre_lower} ' or nombre_lower.startswith(marca + ' '):
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
                return marca.title()

    return 'Otra marca'

# ============================================ #
#          GENERACIÓN DE IDs ÚNICOS            #
# ============================================ #

def generar_id_consistente(nombre):
    """Genera un ID único y consistente basado en el nombre del producto"""
    nombre_normalizado = str(nombre).lower().strip()
    nombre_normalizado = re.sub(r'\s+', ' ', nombre_normalizado)
    
    hash_obj = hashlib.md5(nombre_normalizado.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()
    
    return hash_hex[:12]

# ============================================ #
#          LIMPIEZA DE PRECIOS                 #
# ============================================ #

def limpiar_precio(precio_texto):
    """
    Limpia un texto de precio y lo convierte a float
    """
    if not precio_texto or precio_texto == "Precio no disponible":
        return None
    
    try:
        # Eliminar todo excepto números y comas
        precio_limpio = re.sub(r'[^\d,]', '', str(precio_texto))
        # Convertir coma a punto
        precio_limpio = precio_limpio.replace(',', '.')
        # Convertir a float
        return float(precio_limpio)
    except:
        return None

def limpiar_columna_precio(df):
    """
    Limpia las columnas de precio y las renombra para mantener compatibilidad con histórico
    
    FORMATO FINAL:
    - precio: precio actual/rebajado (numérico)
    - precio_rebajado: precio original tachado (numérico)
    - precio_original_texto: texto del precio actual
    - precio_rebajado_texto: texto del precio original
    """
    print("\n" + "="*60)
    print("LIMPIANDO COLUMNAS DE PRECIO")
    print("="*60)
    
    try:
        # PASO 1: Guardar textos originales con nombres temporales
        if 'precio_actual_temp' in df.columns:
            df['texto_precio_actual_temp'] = df['precio_actual_temp'].copy()
        if 'precio_original_temp' in df.columns:
            df['texto_precio_original_temp'] = df['precio_original_temp'].copy()
        
        # PASO 2: Limpiar precio actual (será la columna 'precio')
        if 'precio_actual_temp' in df.columns:
            print("📊 Limpiando precio actual (precio)...")
            df['precio'] = df['precio_actual_temp'].apply(limpiar_precio)
            
            validos = df['precio'].notna().sum()
            print(f"   ✅ {validos} precios actuales válidos")
            if validos > 0:
                print(f"   📈 Rango: {df['precio'].min():.2f}€ - {df['precio'].max():.2f}€")
                print(f"   📊 Promedio: {df['precio'].mean():.2f}€")
        
        # PASO 3: Limpiar precio original (será la columna 'precio_rebajado')
        if 'precio_original_temp' in df.columns:
            print("📊 Limpiando precio original (precio_rebajado)...")
            df['precio_rebajado'] = df['precio_original_temp'].apply(limpiar_precio)
            
            validos = df['precio_rebajado'].notna().sum()
            print(f"   ✅ {validos} precios originales válidos")
            if validos > 0:
                print(f"   📈 Rango: {df['precio_rebajado'].min():.2f}€ - {df['precio_rebajado'].max():.2f}€")
                print(f"   📊 Promedio: {df['precio_rebajado'].mean():.2f}€")
        
        # PASO 4: Renombrar columnas de texto correctamente
        if 'texto_precio_actual_temp' in df.columns:
            df['precio_original_texto'] = df['texto_precio_actual_temp']
        if 'texto_precio_original_temp' in df.columns:
            df['precio_rebajado_texto'] = df['texto_precio_original_temp']
        
        # PASO 5: Eliminar columnas temporales
        columnas_temp = ['precio_actual_temp', 'precio_original_temp', 
                        'texto_precio_actual_temp', 'texto_precio_original_temp']
        df = df.drop(columns=[col for col in columnas_temp if col in df.columns], errors='ignore')
        
        # PASO 6: Calcular descuentos (precio_rebajado > precio)
        if 'precio_rebajado' in df.columns and 'precio' in df.columns:
            mask = (df['precio_rebajado'].notna()) & (df['precio'].notna()) & (df['precio_rebajado'] > df['precio'])
            
            if mask.sum() > 0:
                df.loc[mask, 'descuento_euros'] = df.loc[mask, 'precio_rebajado'] - df.loc[mask, 'precio']
                df.loc[mask, 'descuento_porcentaje'] = ((df.loc[mask, 'precio_rebajado'] - df.loc[mask, 'precio']) / df.loc[mask, 'precio_rebajado']) * 100
                
                print(f"\n💰 Productos con descuento: {mask.sum()}")
                print(f"   📉 Descuento promedio: {df.loc[mask, 'descuento_porcentaje'].mean():.1f}%")
                print(f"   💵 Ahorro promedio: {df.loc[mask, 'descuento_euros'].mean():.2f}€")
        
        return df
        
    except Exception as e:
        print(f"❌ Error limpiando columnas precio: {e}")
        import traceback
        traceback.print_exc()
        return df

# ============================================ #
#          GOOGLE DRIVE FUNCTIONS              #
# ============================================ #

def configurar_google_drive():
    """Configura y autentica con Google Drive"""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        credenciales_json = (
            os.environ.get('GOOGLE_CREDENTIALS_JSON_2')
            or os.environ.get('GOOGLE_CREDENTIALS_JSON'))
        
        if not credenciales_json:
            print("⚠️  No se encontraron credenciales de Google Drive")
            return None
        
        creds_dict = json.loads(credenciales_json)
        scopes = ['https://www.googleapis.com/auth/drive']
        
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=scopes
        )
        
        service = build('drive', 'v3', credentials=credentials)
        print("✅ Google Drive configurado exitosamente")
        return service
        
    except ImportError:
        print("❌ Módulos de Google API no instalados")
        return None
    except Exception as e:
        print(f"❌ Error configurando Google Drive: {e}")
        return None

def buscar_archivo_drive(service, nombre_archivo, folder_id):
    """Busca un archivo en Google Drive"""
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
    """Descarga un archivo de Google Drive"""
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import io

        metadata = service.files().get(
            fileId=file_id,
            fields="mimeType"
        ).execute()

        mime_type = metadata.get("mimeType")
        fh = io.BytesIO()

        if mime_type == "application/vnd.google-apps.spreadsheet":
            print("📄 Archivo es Google Sheets, exportando como CSV")
            request = service.files().export(
                fileId=file_id,
                mimeType="text/csv"
            )
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
            return contenido_bytes.decode("latin-1")

    except Exception as e:
        print(f"❌ Error descargando archivo de Drive: {e}")
        return None

def subir_archivo_drive(service, nombre_archivo, contenido_csv, folder_id, file_id=None):
    """Sube o actualiza un archivo CSV en Google Drive"""
    try:
        from googleapiclient.http import MediaIoBaseUpload
        import io
        
        csv_bytes = contenido_csv.encode('utf-8')
        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes), 
            mimetype='text/csv',
            resumable=False
        )
        
        if file_id:
            print(f"📤 Actualizando archivo existente en Drive (ID: {file_id})")
            file = service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
            print("✅ Archivo actualizado en Drive")
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

def actualizar_csv_drive(
    df_nuevo,
    folder_id="1cSW4uOfw4x61a-R6TAOyn6ejEHNiyX0v",
    nombre_archivo="ebooks_mediamarkt.csv"
):
    """Actualiza el CSV histórico en Google Drive"""
    import io
    import pandas as pd

    def leer_csv_seguro(contenido):
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

    print(f"🧹 Duplicados eliminados: {filas_antes - filas_despues}")
    print(f"📊 Total final en histórico: {len(df_combinado)}")

    csv_contenido = df_combinado.to_csv(index=False, encoding="utf-8")

    if archivo_existente:
        subir_exitoso = subir_archivo_drive(
            service,
            nombre_archivo,
            csv_contenido,
            folder_id,
            archivo_existente["id"]
        )
    else:
        subir_exitoso = subir_archivo_drive(
            service,
            nombre_archivo,
            csv_contenido,
            folder_id,
            None
        )

    if subir_exitoso:
        print("✅ Histórico actualizado correctamente en Google Drive")
        return True
    else:
        print("❌ Error subiendo el archivo a Drive")
        return False

# ============================================ #
#          FUNCIONES DE SCRAPING               #
# ============================================ #

def setup_chrome_options():
    """Configura Chrome para ejecución headless"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    return chrome_options

def mediamark_mob_(url):
    """Inicializa el navegador Chrome"""
    try:
        chrome_options = setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        driver.get(url)
        time.sleep(2)

        try:
            aceptar = driver.find_element(By.ID, "pwa-consent-layer-accept-all-button")
            aceptar.click()
            print("✅ Cookies aceptadas")
        except Exception as e:
            print(f"⚠️ Error aceptando cookies: {e}")

        time.sleep(3)
        return driver
        
    except Exception as e:
        print(f"❌ Error inicializando Chrome: {e}")
        raise

def obtener_total_articulos(driver):
    """Obtiene el número total de artículos"""
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

def extraer_precios_producto(contenedor_producto):
    """
    Extrae AMBOS precios: actual y original (tachado)
    
    Returns:
        tuple: (precio_actual, precio_original_tachado)
        
    NOTA: Los nombres aquí son descriptivos, luego se renombrarán en guardar_en_dataframe()
    para mantener compatibilidad con el formato antiguo del CSV
    """
    precio_actual = "Precio no disponible"
    precio_original_tachado = None
    
    try:
        # 1. PRECIO ACTUAL (precio de venta - color rojo/destacado)
        # Buscar: sc-94eb08bc-0 iJxYPS o similar
        selectores_actual = [
            'span.sc-94eb08bc-0.iJxYPS',
            'span.sc-94eb08bc-0.dYbTef.sc-8a3a8cd8-2.csCDkt',
            'span[class*="sc-94eb08bc-0"][class*="iJxYPS"]',
        ]
        
        for selector in selectores_actual:
            try:
                elemento = contenedor_producto.find_element(By.CSS_SELECTOR, selector)
                precio_actual = elemento.text.strip()
                if precio_actual and '€' in precio_actual:
                    break
            except:
                continue
        
        # 2. PRECIO ORIGINAL TACHADO (precio antes del descuento - gris tachado)
        # Buscar: sc-94eb08bc-0 dYbTef sc-a69e154d-2 dJKnju
        selectores_original_tachado = [
            'span.sc-94eb08bc-0.dYbTef.sc-a69e154d-2.dJKnju',
            'span.sc-94eb08bc-0.OhHlB.sc-8a3a8cd8-2.csCDkt',
            'span[class*="sc-a69e154d-2"]',
            'span[class*="dJKnju"]',
        ]
        
        for selector in selectores_original_tachado:
            try:
                elemento = contenedor_producto.find_element(By.CSS_SELECTOR, selector)
                precio_original_tachado = elemento.text.strip()
                if precio_original_tachado and '€' in precio_original_tachado:
                    break
            except:
                continue
        
        # 3. Si no encontramos precio actual, buscar cualquier precio
        if precio_actual == "Precio no disponible":
            try:
                elementos_precio = contenedor_producto.find_elements(By.XPATH, ".//*[contains(text(), '€')]")
                for elem in elementos_precio:
                    texto = elem.text.strip()
                    if '€' in texto and any(c.isdigit() for c in texto):
                        precio_actual = texto
                        break
            except:
                pass
        
        return precio_actual, precio_original_tachado
        
    except Exception as e:
        return f"Error: {e}", None

def extraer_link_producto(contenedor_producto, driver, profundidad=0, max_profundidad=3):
    """Extrae el enlace del producto"""
    if profundidad > max_profundidad:
        return "No disponible"
    
    try:
        WebDriverWait(driver, 2).until(
            lambda d: contenedor_producto.is_displayed()
        )
    except Exception:
        pass
    
    try:
        enlace_element = WebDriverWait(contenedor_producto, 2).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-test="mms-router-link-product-list-item-link_mp"]'))
        )
        href = enlace_element.get_attribute('href')
        if href and 'mediamarkt' in href:
            resultado = href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
            return resultado
    except Exception:
        pass
    
    posibles_selectores = [
        ('a[href*="/p/"]', "Href /p/"),
        ('a[href*="/product/"]', "Href /product/"),
        ('a[data-test*="product-list-item-link"]', "data-test genérico"),
        ('a', "Cualquier enlace"),
    ]
    
    for selector, descripcion in posibles_selectores:
        try:
            elementos = contenedor_producto.find_elements(By.CSS_SELECTOR, selector)
            for elemento in elementos:
                href = elemento.get_attribute("href")
                if not href or "mediamarkt" not in href:
                    continue
                if "/p/" not in href and "/product/" not in href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.mediamarkt.es" + href
                return href
        except Exception:
            continue
    
    if profundidad < max_profundidad:
        try:
            padre = contenedor_producto.find_element(By.XPATH, "./..")
            return extraer_link_producto(padre, driver, profundidad+1, max_profundidad)
        except Exception:
            pass
    
    return "No disponible"

def extraer_productos_pagina(driver):
    """Extrae los productos de una sola página"""
    productos_pagina = []

    try:
        titulos = driver.find_elements(By.CSS_SELECTOR, 'p[data-test="product-title"]')
        print(f"   🔍 Encontrados {len(titulos)} productos en la página")

        for i, titulo in enumerate(titulos, start=1):
            try:
                nombre = titulo.text.strip()

                # ENLACE
                try:
                    enlace_elem = titulo.find_element(By.XPATH, ".//ancestor::a[1]")
                    enlace = enlace_elem.get_attribute("href")
                    if enlace and not enlace.startswith("http"):
                        enlace = "https://www.mediamarkt.es" + enlace
                except Exception:
                    enlace = "No disponible"

                # CONTENEDOR
                contenedor = titulo
                for _ in range(5):
                    contenedor = contenedor.find_element(By.XPATH, "./..")
                    precios = contenedor.find_elements(By.XPATH, ".//*[contains(text(), '€')]")
                    if precios:
                        break

                # PRECIOS (AMBOS)
                # Usamos nombres temporales que luego se renombrarán
                precio_actual, precio_original_tachado = extraer_precios_producto(contenedor)

                # MARCA
                marca = extraer_marca(nombre)

                # ID
                producto_id = generar_id_consistente(nombre)

                productos_pagina.append({
                    'id': producto_id,
                    'nombre': nombre,
                    'precio_actual_temp': precio_actual,  # Será 'precio'
                    'precio_original_temp': precio_original_tachado,  # Será 'precio_rebajado'
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
    """Extrae todos los productos"""
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
            print(f"\n🎯 Usando criterio de ordenación: {criterio}")
            
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
                        print(f"❌ La página {pagina} no cargó correctamente")
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
        
        print(f"\n📊 Resumen final: {len(productos_data)} productos únicos")
        
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
    
    FORMATO FINAL DE COLUMNAS (compatible con histórico):
    - fecha_extraccion
    - id
    - numero
    - nombre
    - marca
    - precio (precio actual/rebajado)
    - enlace
    - precio_rebajado (precio original tachado)
    - descuento_euros (opcional)
    - descuento_porcentaje (opcional)
    - precio_original_texto (texto del precio actual)
    - precio_rebajado_texto (texto del precio original)
    """
    if not productos_data:
        print("No hay datos para guardar")
        return None, None
    
    df = pd.DataFrame(productos_data)
    
    fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['fecha_extraccion'] = fecha_extraccion
    
    if 'id' not in df.columns:
        print("\n" + "="*60)
        print("GENERANDO IDs ÚNICOS PARA PRODUCTOS")
        print("="*60)
        df['id'] = df['nombre'].apply(generar_id_consistente)
        print(f"✅ IDs generados para {len(df)} productos")
        print(f"📊 IDs únicos: {df['id'].nunique()}")
    
    # Limpiar columnas de precio (esto renombra las columnas correctamente)
    df = limpiar_columna_precio(df)
    
    # Orden de columnas (formato antiguo compatible)
    column_order = [
        'fecha_extraccion', 'id', 'numero', 'nombre', 'marca',
        'precio', 'enlace', 'precio_rebajado'
    ]
    
    # Agregar columnas calculadas si existen
    if 'descuento_euros' in df.columns:
        column_order.append('descuento_euros')
    if 'descuento_porcentaje' in df.columns:
        column_order.append('descuento_porcentaje')
    
    # Agregar columnas de texto al final
    if 'precio_original_texto' in df.columns:
        column_order.append('precio_original_texto')
    if 'precio_rebajado_texto' in df.columns:
        column_order.append('precio_rebajado_texto')
    
    # Asegurar que todas las columnas existan
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]
    
    os.makedirs("scraping_results", exist_ok=True)

    nombre_archivo = f"scraping_results/ebooks_mediamarkt_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(nombre_archivo, index=False, encoding='utf-8')
    
    print(f"\n✅ Datos guardados en: {nombre_archivo}")
    print(f"📊 Total de productos únicos: {len(df)}")
    
    # Estadísticas de marcas
    print(f"\n🏷️  Distribución de marcas:")
    distribucion_marcas = df['marca'].value_counts()
    for marca, cantidad in distribucion_marcas.head(10).items():
        print(f"   {marca}: {cantidad} productos")
    
    if len(distribucion_marcas) > 10:
        print(f"   ... y {len(distribucion_marcas) - 10} marcas más")
    
    # Estadísticas de IDs
    print(f"\n🔑 Estadísticas de IDs:")
    print(f"   IDs únicos: {df['id'].nunique()}")
    productos_duplicados = len(df) - df['id'].nunique()
    if productos_duplicados > 0:
        print(f"   ⚠️  Productos duplicados (mismo ID): {productos_duplicados}")
    
    # Estadísticas de precios
    if 'precio' in df.columns:
        productos_con_precio = df['precio'].notna().sum()
        print(f"\n💰 Productos con precio válido: {productos_con_precio}")
        
        if productos_con_precio > 0:
            print(f"   📈 Precio promedio: {df['precio'].mean():.2f}€")
            print(f"   📊 Precio mediano: {df['precio'].median():.2f}€")
            print(f"   📉 Precio mínimo: {df['precio'].min():.2f}€")
            print(f"   📈 Precio máximo: {df['precio'].max():.2f}€")
    
    # Estadísticas de descuentos
    if 'descuento_porcentaje' in df.columns:
        productos_con_descuento = df['descuento_porcentaje'].notna().sum()
        if productos_con_descuento > 0:
            print(f"\n💸 Productos con descuento: {productos_con_descuento}")
            print(f"   📉 Descuento promedio: {df['descuento_porcentaje'].mean():.1f}%")
            print(f"   💵 Ahorro promedio: {df['descuento_euros'].mean():.2f}€")
            print(f"   🎯 Mayor descuento: {df['descuento_porcentaje'].max():.1f}%")
    
    # Estadísticas de precios originales
    if 'precio_rebajado' in df.columns:
        productos_con_precio_original = df['precio_rebajado'].notna().sum()
        if productos_con_precio_original > 0:
            print(f"\n🏷️  Productos con precio original (tachado): {productos_con_precio_original}")
    
    # Estadísticas de enlaces
    if 'enlace' in df.columns:
        enlaces_validos = df[df['enlace'] != 'No disponible']['enlace'].count()
        print(f"\n🔗 Enlaces extraídos: {enlaces_validos} de {len(df)} productos")
    
    print("\n📋 Primeras 5 filas del DataFrame:")
    print(df.head())
    
    print("\n📋 Estructura de columnas:")
    print(f"   Columnas: {list(df.columns)}")
    
    return df, nombre_archivo

# ============================================ #
#          FUNCION PRINCIPAL                   #
# ============================================ #

def main():
    """Función principal"""
    print("="*60)
    print("SCRAPING DE EBOOKS - MEDIAMARKT")
    print("Con extracción de PRECIO ACTUAL y PRECIO ORIGINAL")
    print("="*60)
    print(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    driver = None
    
    try:
        url = "https://www.mediamarkt.es/es/category/ebooks-249.html?sort=currentprice+desc"
        
        print(f"\n🌐 Accediendo a: {url}")
        
        driver = mediamark_mob_(url)
        
        productos_data = extraer_productos(driver)
        
        if not productos_data:
            print("❌ No se extrajeron productos")
            return False
        
        df, archivo_csv = guardar_en_dataframe(productos_data)
        
        if df is None:
            print("❌ Error creando DataFrame. Terminando ejecución.")
            return False
        
        print("\n🔄 Actualizando Google Drive (APPEND mode)...")
        print("📌 Nota: Los datos se añadirán, NO se sobrescribirán")
        print("📌 Se mantendrá el historial día a día")
        
        drive_actualizado = actualizar_csv_drive(df)
        
        if drive_actualizado:
            print("✅ Google Drive actualizado exitosamente (APPEND)")
        else:
            print("⚠️  No se pudo actualizar Google Drive")
        
        print("\n" + "="*60)
        print("RESUMEN EJECUCIÓN")
        print("="*60)
        print(f"✅ Scraping completado exitosamente")
        print(f"📦 Productos obtenidos hoy: {len(df)}")
        print(f"🔑 IDs únicos generados: {df['id'].nunique()}")
        print(f"🏷️  Marcas diferentes encontradas: {df['marca'].nunique()}")
        
        if 'precio' in df.columns:
            precios_validos = df['precio'].notna().sum()
            print(f"💰 Precios actuales válidos: {precios_validos}")
        
        if 'precio_rebajado' in df.columns:
            precios_originales = df['precio_rebajado'].notna().sum()
            print(f"🏷️  Precios originales (tachados) extraídos: {precios_originales}")
        
        if 'descuento_porcentaje' in df.columns:
            productos_descuento = df['descuento_porcentaje'].notna().sum()
            print(f"💸 Productos con descuento: {productos_descuento}")
        
        if 'enlace' in df.columns:
            enlaces_validos = df[df['enlace'] != 'No disponible']['enlace'].count()
            print(f"🔗 Enlaces extraídos: {enlaces_validos}")
        
        print(f"📁 Archivo local generado: {archivo_csv}")
        print(f"💾 Google Drive: Datos añadidos al archivo histórico")
        
        print("\n📋 ESTRUCTURA FINAL DEL CSV:")
        print("   - precio: precio actual de venta")
        print("   - precio_rebajado: precio original (antes del descuento)")
        print("   - Compatible con formato histórico antiguo ✅")
        
        return True
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
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

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
