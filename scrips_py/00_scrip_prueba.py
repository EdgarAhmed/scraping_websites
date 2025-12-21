#!/usr/bin/env python3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import os
import re
import sys
import hashlib

# =========================
# CONFIG
# =========================
MODO_PRUEBA = True
MAX_PAGINAS = 2
SCROLLS_POR_PAGINA = 4
TIEMPO_SCROLL = 1.5

# =========================
# UTILS
# =========================
def generar_id(nombre):
    return hashlib.md5(nombre.lower().strip().encode()).hexdigest()[:12]

def limpiar_precio(texto):
    if not texto:
        return None
    m = re.search(r"(\d+[,.]\d+)", texto)
    return m.group(1).replace(",", ".") if m else None

# =========================
# CHROME
# =========================
def setup_driver():
    options = Options()
    # IMPORTANTE: no headless
    # options.add_argument("--headless")

    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# =========================
# SCRAPING
# =========================
def aceptar_cookies(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "pwa-consent-layer-accept-all-button"))
        ).click()
        time.sleep(1)
    except:
        pass

def scroll_render(driver):
    for _ in range(SCROLLS_POR_PAGINA):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(TIEMPO_SCROLL)

def extraer_productos(driver):
    productos = []
    vistos = set()

    for pagina in range(1, MAX_PAGINAS + 1):
        url = f"https://www.mediamarkt.es/es/category/ebooks-249.html?page={pagina}"
        print(f"\n📖 Página {pagina}")
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        scroll_render(driver)

        enlaces = driver.find_elements(By.XPATH, "//a[contains(@href,'/p/')]")
        print(f"   🔗 Enlaces /p/ encontrados: {len(enlaces)}")

        for a in enlaces:
            try:
                href = a.get_attribute("href")
                if not href or href in vistos:
                    continue

                contenedor = a.find_element(By.XPATH, "./ancestor::div[1]")
                texto = contenedor.text.strip()
                if len(texto) < 10:
                    continue

                nombre = texto.split("\n")[0]
                precio = limpiar_precio(texto)

                productos.append({
                    "id": generar_id(nombre),
                    "nombre": nombre,
                    "precio": precio,
                    "enlace": href
                })
                vistos.add(href)

            except:
                continue

    return productos

# =========================
# MAIN
# =========================
def main():
    driver = setup_driver()
    try:
        driver.get("https://www.mediamarkt.es/es/category/ebooks-249.html")
        aceptar_cookies(driver)

        productos = extraer_productos(driver)

        if not productos:
            print("\n❌ MediaMarkt no devolvió productos (DOM vacío)")
            return False

        df = pd.DataFrame(productos)
        df["fecha_extraccion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        os.makedirs("scraping_results", exist_ok=True)
        ruta = f"scraping_results/ebooks_{datetime.now().strftime('%H%M%S')}.csv"
        df.to_csv(ruta, index=False, encoding="utf-8")

        print("\n✅ EXTRACCIÓN OK")
        print(df.head(3))
        print(f"\n💾 Guardado en {ruta}")
        return True

    finally:
        driver.quit()
        print("\n🛑 Navegador cerrado")

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
