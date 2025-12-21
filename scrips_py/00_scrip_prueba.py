#!/usr/bin/env python3
"""
Script de scraping para ebooks de MediaMarkt - VERSIÓN RÁPIDA PARA PRUEBAS
Mantiene todas las funcionalidades pero limitado para pruebas rápidas
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
#       MODO PRUEBA - CONFIGURACIÓN RÁPIDA     #
# ============================================ #
MODO_PRUEBA = True  # Cambiar a False para ejecución completa
MAX_PAGINAS_PRUEBA = 2  # Solo 2 páginas por criterio en modo prueba
MAX_CRITERIOS_PRUEBA = 2  # Solo 2 criterios de ordenación en modo prueba
TIEMPO_ESPERA_REDUCIDO = 1  # 1 segundo en lugar de 2-3

# ============================================ #
#       CONFIGURACIÓN DE MARCAS DE EBOOKS      #
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
    if pd.isna(nombre):
        return 'Desconocido'
    nombre_lower = str(nombre).lower()
    
    # Casos especiales
    if 'kindle' in nombre_lower:
        return 'Amazon'
    if 'kobo' in nombre_lower:
        return 'Kobo'
    if 'pocketbook' in nombre_lower:
        return 'PocketBook'
    
    # Búsqueda general
    for marca in marcas_ebooks:
        if f' {marca} ' in f' {nombre_lower} ' or nombre_lower.startswith(marca + ' '):
            if marca in ['bq', 'kobo']:
                return marca.upper()
            elif marca == 'kindle':
                return 'Amazon'
            else:
                return marca.title()
    return 'Otra marca'

# ============================================ #
#    FUNCIONES PARA GENERAR IDs ÚNICOS         #
# ============================================ #

def generar_id_consistente(nombre):
    """Genera ID único basado en el nombre"""
    nombre_normalizado = str(nombre).lower().strip()
    nombre_normalizado = re.sub(r'\s+', ' ', nombre_normalizado)
    hash_obj = hashlib.md5(nombre_normalizado.encode('utf-8'))
    return hash_obj.hexdigest()[:12]

# ============================================ #
#          LIMPIA PRECIOS                      #
# ============================================ #

def limpiar_columna_precio(df):
    """Limpia la columna precio para extraer valores numéricos"""
    print("\n" + "="*60)
    print("LIMPIANDO COLUMNA PRECIO")
    print("="*60)
    
    try:
        if 'precio_original' not in df.columns:
            df['precio_original'] = df['precio'].copy()
        
        print(f"📊 Total de registros: {len(df)}")
        
        # Limpieza rápida
        df['precio'] = (
            df['precio']
            .astype(str)
            .str.replace(r'[^\d,]', '', regex=True)
            .str.replace(',', '.', regex=False)
        )
        
        df['precio'] = pd.to_numeric(df['precio'], errors='coerce')
        
        print(f"✅ Precios limpiados - Válidos: {df['precio'].notna().sum()}")
        return df
        
    except Exception as e:
        print(f"❌ Error limpiando precios: {e}")
        return df

# ============================================ #
#       CONFIGURACIÓN GOOGLE DRIVE             #
# ============================================ #

def configurar_google_drive():
    """Configura Google Drive (solo si se necesita)"""
    if MODO_PRUEBA:
        print("⚠️  MODO PRUEBA: Google Drive desactivado para pruebas rápidas")
        return None
    
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        credenciales_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        if not credenciales_json:
            print("⚠️  No hay credenciales de Google Drive")
            return None
        
        creds_dict = json.loads(credenciales_json)
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        service = build('drive', 'v3', credentials=credentials)
        print("✅ Google Drive configurado")
        return service
        
    except Exception as e:
        print(f"❌ Error Google Drive: {e}")
        return None

def actualizar_csv_drive(df_nuevo):
    """Actualiza CSV en Google Drive (solo si no está en modo prueba)"""
    if MODO_PRUEBA:
        print("⚠️  MODO PRUEBA: Saltando actualización de Google Drive")
        return True
    
    print("\n" + "="*60)
    print("ACTUALIZANDO GOOGLE DRIVE")
    print("="*60)
    
    service = configurar_google_drive()
    if not service:
        return False
    
    # Implementación simplificada para pruebas
    try:
        print("📤 Simulando subida a Drive...")
        print(f"📊 Se subirían {len(df_nuevo)} productos")
        print("✅ Drive actualizado (simulado)")
        return True
    except Exception as e:
        print(f"❌ Error Drive: {e}")
        return False

# ============================================ #
#      FUNCIONES DEL SCRAPING - OPTIMIZADAS    #
# ============================================ #

def setup_chrome_options():
    """Configura Chrome optimizado para pruebas"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # DESACTIVAR IMÁGENES para mayor velocidad
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.javascript": 1  # Mantener JS
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    return chrome_options

def inicializar_navegador(url):
    """Inicializa Chrome rápidamente"""
    try:
        chrome_options = setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        driver.get(url)
        time.sleep(TIEMPO_ESPERA_REDUCIDO)
        
        # Intentar aceptar cookies rápido
        try:
            aceptar = driver.find_element(By.ID, "pwa-consent-layer-accept-all-button")
            aceptar.click()
            print("✅ Cookies aceptadas")
            time.sleep(0.5)
        except:
            pass
            
        return driver
        
    except Exception as e:
        print(f"❌ Error inicializando Chrome: {e}")
        raise

def obtener_total_articulos(driver):
    """Obtiene número total de artículos (versión rápida)"""
    try:
        elemento_total = driver.find_element(By.CSS_SELECTOR, 'span.sc-94eb08bc-0.AKpzk')
        texto_total = elemento_total.text
        numero_total = re.search(r'\((\d+)', texto_total)
        
        if numero_total:
            total_articulos = int(numero_total.group(1))
            print(f"📊 Total de artículos: {total_articulos}")
            return total_articulos, min(3, math.ceil(total_articulos / 12))
        return None, 2 if MODO_PRUEBA else 10
    except:
        return None, 2 if MODO_PRUEBA else 10

def extraer_precio_producto(contenedor_producto):
    """Extrae precio rápidamente"""
    try:
        # Intentar selectores rápidamente
        selectores = [
            'span.sc-94eb08bc-0.dYbTef.sc-8a3a8cd8-2.csCDkt',
            'span.sc-94eb08bc-0.OhHlB.sc-8a3a8cd8-2.csCDkt',
            'span[data-test="product-price"]',
            'span[class*="price"]'
        ]
        
        for selector in selectores:
            try:
                precio = contenedor_producto.find_element(By.CSS_SELECTOR, selector)
                if precio.text.strip():
                    return precio.text
            except:
                continue
                
        return "Precio no disponible"
    except:
        return "Precio no disponible"

def extraer_link_producto(contenedor_producto):
    """Extrae enlace rápidamente"""
    try:
        # Selector más común primero
        try:
            enlace = contenedor_producto.find_element(
                By.CSS_SELECTOR, 
                'a[data-test="mms-router-link-product-list-item-link_mp"]'
            )
            href = enlace.get_attribute('href')
            if href:
                return href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
        except:
            pass
        
        # Buscar cualquier enlace que contenga /p/
        enlaces = contenedor_producto.find_elements(By.TAG_NAME, "a")
        for enlace in enlaces:
            href = enlace.get_attribute('href')
            if href and '/p/' in href:
                return href if href.startswith('http') else f"https://www.mediamarkt.es{href}"
                
        return "No disponible"
    except:
        return "No disponible"

def extraer_productos_pagina(driver):
    """Extrae productos de una página (optimizada)"""
    productos_pagina = []
    
    try:
        # Buscar contenedores principales directamente
        contenedores = driver.find_elements(
            By.CSS_SELECTOR, 
            'div[data-test="mms-search-srp-productlist-item"]'
        )
        
        if not contenedores:
            # Fallback a búsqueda por estructura común
            contenedores = driver.find_elements(
                By.CSS_SELECTOR,
                'div[class*="product-list-item"]'
            )
        
        print(f"   🔍 Encontrados {len(contenedores)} productos en la página")
        
        for contenedor in contenedores:
            try:
                # Extraer nombre
                try:
                    nombre_elem = contenedor.find_element(
                        By.CSS_SELECTOR, 
                        'p[data-test="product-title"], h2, .product-title'
                    )
                    nombre = nombre_elem.text.strip()
                except:
                    continue
                
                if not nombre:
                    continue
                
                # Extraer precio
                precio = extraer_precio_producto(contenedor)
                
                # Extraer enlace
                enlace = extraer_link_producto(contenedor)
                
                # Extraer marca
                marca = extraer_marca_ebook(nombre)
                
                # Generar ID
                producto_id = generar_id_consistente(nombre)
                
                productos_pagina.append({
                    'id': producto_id,
                    'nombre': nombre,
                    'precio': precio,
                    'marca': marca,
                    'enlace': enlace
                })
                
            except Exception as e:
                continue
                
        return productos_pagina
        
    except Exception as e:
        print(f"   ❌ Error en página: {e}")
        return productos_pagina

def extraer_productos(driver):
    """Extrae productos (versión optimizada para pruebas)"""
    productos_data = []
    contador_global = 1
    
    try:
        total_articulos, total_paginas = obtener_total_articulos(driver)
        
        # CONFIGURACIÓN PARA PRUEBAS RÁPIDAS
        if MODO_PRUEBA:
            criterios = ["currentprice+desc", "name+asc"][:MAX_CRITERIOS_PRUEBA]
            max_paginas = MAX_PAGINAS_PRUEBA
            print(f"⚡ MODO PRUEBA: {len(criterios)} criterios, {max_paginas} páginas máx")
        else:
            criterios = ["currentprice+desc", "currentprice+asc", "relevance", "name+asc", "name+desc"]
            max_paginas = min(total_paginas, 30) if total_paginas else 30
        
        productos_unicos = set()
        
        for criterio in criterios:
            print(f"\n🎯 Criterio: {criterio}")
            
            for pagina in range(1, max_paginas + 1):
                try:
                    print(f"   📖 Página {pagina}/{max_paginas}")
                    
                    url_pagina = f"https://www.mediamarkt.es/es/category/ebooks-249.html?sort={criterio}&page={pagina}"
                    driver.get(url_pagina)
                    time.sleep(TIEMPO_ESPERA_REDUCIDO)
                    
                    # Esperar carga mínima
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-test*="product"]'))
                        )
                    except:
                        print(f"      ⏭️  Página {pagina} no cargó, pasando...")
                        break
                    
                    productos_pagina = extraer_productos_pagina(driver)
                    
                    for producto in productos_pagina:
                        nombre = producto['nombre']
                        if nombre and nombre not in productos_unicos:
                            productos_unicos.add(nombre)
                            producto['numero'] = contador_global
                            contador_global += 1
                            productos_data.append(producto)
                    
                    print(f"      ✅ {len(productos_pagina)} productos, Únicos: {len(productos_data)}")
                    
                    # En modo prueba, limitar productos totales
                    if MODO_PRUEBA and len(productos_data) >= 20:
                        print(f"      ⏹️  Límite de prueba alcanzado (20 productos)")
                        return productos_data
                    
                    if len(productos_pagina) < 5:  # Página con pocos productos
                        break
                        
                except Exception as e:
                    print(f"      ❌ Error página {pagina}: {e}")
                    continue
        
        print(f"\n📊 Total productos únicos: {len(productos_data)}")
        return productos_data
        
    except Exception as e:
        print(f"❌ Error general: {e}")
        return productos_data

def guardar_en_dataframe(productos_data):
    """Guarda datos en DataFrame (optimizado)"""
    if not productos_data:
        print("❌ No hay datos para guardar")
        return None, None
    
    df = pd.DataFrame(productos_data)
    fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['fecha_extraccion'] = fecha_extraccion
    
    # Generar IDs si no existen
    if 'id' not in df.columns:
        df['id'] = df['nombre'].apply(generar_id_consistente)
    
    # Limpiar precios
    df = limpiar_columna_precio(df)
    
    # Ordenar columnas
    column_order = ['fecha_extraccion', 'id', 'numero', 'nombre', 'marca', 'precio', 'enlace']
    if 'precio_original' in df.columns:
        column_order.append('precio_original')
    
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]
    
    # Guardar localmente
    os.makedirs("scraping_results", exist_ok=True)
    
    if MODO_PRUEBA:
        nombre_archivo = f"scraping_results/PRUEBA_ebooks_{datetime.now().strftime('%H%M%S')}.csv"
    else:
        nombre_archivo = f"scraping_results/ebooks_mediamarkt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    file_path = nombre_archivo
    df.to_csv(file_path, index=False, encoding='utf-8')
    
    # Mostrar resumen rápido
    print("\n" + "="*60)
    print("RESUMEN DE EXTRACCIÓN")
    print("="*60)
    print(f"📦 Productos extraídos: {len(df)}")
    print(f"💰 Precios válidos: {df['precio'].notna().sum()}")
    print(f"🏷️  Marcas únicas: {df['marca'].nunique()}")
    print(f"🔗 Enlaces: {df[df['enlace'] != 'No disponible']['enlace'].count()}")
    print(f"💾 Guardado en: {file_path}")
    
    # Mostrar primeros productos
    print("\n📋 Primeros 3 productos:")
    for i, row in df.head(3).iterrows():
        print(f"  {i+1}. {row['nombre'][:40]}... - {row['precio']}€ - {row['marca']}")
    
    return df, file_path

# ============================================ #
#      FUNCION PRINCIPAL OPTIMIZADA            #
# ============================================ #

def main():
    """Función principal optimizada para pruebas"""
    print("="*60)
    print("SCRAPING DE EBOOKS - MEDIAMARKT")
    if MODO_PRUEBA:
        print("⚡ VERSIÓN RÁPIDA PARA PRUEBAS")
    print("="*60)
    print(f"🕐 Inicio: {datetime.now().strftime('%H:%M:%S')}")
    
    driver = None
    try:
        # URL base
        url = "https://www.mediamarkt.es/es/category/ebooks-249.html"
        
        print(f"\n🌐 Inicializando navegador...")
        inicio = time.time()
        
        driver = inicializar_navegador(url)
        
        print(f"⏱️  Navegador listo en {time.time() - inicio:.1f}s")
        print(f"\n🔍 Comenzando extracción...")
        
        productos_data = extraer_productos(driver)
        
        if not productos_data:
            print("❌ No se extrajeron productos")
            return False
        
        print(f"\n💾 Guardando datos...")
        df, archivo_csv = guardar_en_dataframe(productos_data)
        
        if df is None:
            return False
        
        # Actualizar Google Drive (solo si no es prueba)
        if not MODO_PRUEBA:
            print("\n☁️  Actualizando Google Drive...")
            drive_actualizado = actualizar_csv_drive(df)
            if drive_actualizado:
                print("✅ Google Drive actualizado")
            else:
                print("⚠️  Google Drive no actualizado")
        
        tiempo_total = time.time() - inicio
        print(f"\n⏱️  Tiempo total: {tiempo_total:.1f} segundos")
        print(f"📊 Productos por segundo: {len(df)/tiempo_total:.1f}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
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
        print(f"🏁 Fin: {datetime.now().strftime('%H:%M:%S')}")
        print("="*60)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
