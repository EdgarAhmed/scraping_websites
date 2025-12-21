#!/usr/bin/env python3
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
import hashlib

# ================================
# CONFIG PRUEBA
# ================================
MODO_PRUEBA = True
MAX_PAGINAS_PRUEBA = 2
MAX_CRITERIOS_PRUEBA = 2
TIEMPO_ESPERA = 2

# ================================
# MARCAS
# ================================
marcas_ebooks = [
    'kindle', 'kobo', 'pocketbook', 'onyx', 'remarkable', 'sony',
    'nook', 'tolino', 'bq', 'inkbook', 'bookeen', 'energy'
]

def extraer_marca_ebook(nombre):
    if not nombre:
        return "Desconocido"
    n = nombre.lower()
    if "kindle" in n:
        return "Amazon"
    if "kobo" in n:
        return "Kobo"
    if "pocketbook" in n:
        return "PocketBook"
    for m in marcas_ebooks:
        if m in n:
            return m.title()
    return "Otra marca"

def generar_id_consistente(nombre):
    return hashlib.md5(nombre.lower().strip().encode()).hexdigest()[:12]

# ================================
# CHROME
# ================================
def setup_chrome_options():
    options = Options()
    # ❌ NO headless
    # options.add_argument("--headless")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )

    prefs = {
        "profile.managed_default_content_settings.images": 2
    }
    options.add_experimental_option("prefs", prefs)
    return options

def inicializar_navegador(url):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=setup_chrome_options())
    driver.get(url)
    time.sleep(3)

    try:
        driver.find_element(By.ID, "pwa-consent-layer-accept-all-button").click()
        time.sleep(1)
    except:
        pass

    return driver

# ================================
# SCRAPING
# ================================
def scroll_completo(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

def extraer_precio(contenedor):
    try:
        spans = contenedor.find_elements(By.TAG_NAME, "span")
        for s in spans:
            t = s.text.strip()
            if re.search(r"\d+[,\.]\d+", t):
                return t
    except:
        pass
    return "No disponible"

def extraer_link(contenedor):
    try:
        enlaces = contenedor.find_elements(By.TAG_NAME, "a")
        for a in enlaces:
            href = a.get_attribute("href")
            if href and "/p/" in href:
                return href
    except:
        pass
    return "No disponible"

def extraer_productos_pagina(driver):
    productos = []

    scroll_completo(driver)

    contenedores = driver.find_elements(
        By.CSS_SELECTOR,
        "article, div[class*='product'], div[class*='Product']"
    )

    print(f"   🔍 Contenedores detectados: {len(contenedores)}")

    for c in contenedores:
        try:
            link = extraer_link(c)
            if link == "No disponible":
                continue

            nombre = c.text.split("\n")[0].strip()
            if len(nombre) < 10:
                continue

            productos.append({
                "id": generar_id_consistente(nombre),
                "nombre": nombre,
                "precio": extraer_precio(c),
                "marca": extraer_marca_ebook(nombre),
                "enlace": link
            })
        except:
            continue

    return productos

def extraer_productos(driver):
    productos_finales = []
    vistos = set()

    criterios = ["currentprice+desc", "name+asc"][:MAX_CRITERIOS_PRUEBA]
    max_paginas = MAX_PAGINAS_PRUEBA

    for criterio in criterios:
        print(f"\n🎯 Criterio: {criterio}")
        for pagina in range(1, max_paginas + 1):
            url = f"https://www.mediamarkt.es/es/category/ebooks-249.html?sort={criterio}&page={pagina}"
            print(f"   📖 Página {pagina}")
            driver.get(url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            productos = extraer_productos_pagina(driver)

            for p in productos:
                if p["id"] not in vistos:
                    vistos.add(p["id"])
                    p["numero"] = len(productos_finales) + 1
                    productos_finales.append(p)

            print(f"      ✅ Nuevos: {len(productos)} | Total únicos: {len(productos_finales)}")

    return productos_finales

# ================================
# MAIN
# ================================
def main():
    driver = None
    try:
        driver = inicializar_navegador(
            "https://www.mediamarkt.es/es/category/ebooks-249.html"
        )

        productos = extraer_productos(driver)

        if not productos:
            print("❌ No se extrajeron productos")
            return False

        df = pd.DataFrame(productos)
        df["fecha_extraccion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        os.makedirs("scraping_results", exist_ok=True)
        path = f"scraping_results/ebooks_test_{datetime.now().strftime('%H%M%S')}.csv"
        df.to_csv(path, index=False, encoding="utf-8")

        print("\n📊 RESULTADO FINAL")
        print(df.head(3))
        print(f"\n💾 Guardado en {path}")
        return True

    finally:
        if driver:
            driver.quit()
            print("\n🛑 Navegador cerrado")

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
