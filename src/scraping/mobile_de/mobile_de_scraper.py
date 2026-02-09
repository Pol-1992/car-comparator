#!/usr/bin/env python3
"""
Mobile.de Robust Scraper V2
Con inspección de HTML y selectores dinámicos
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from typing import List, Dict, Optional
import json
from datetime import datetime
import os
import re
from urllib.parse import urlencode, urlparse, parse_qs

class MobileDeScraper:
    def __init__(self, base_url: str, output_dir: str = "mobile_de_data"):
        self.base_url = base_url
        self.output_dir = output_dir
        self.session = requests.Session()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        ]
        
        # Stats
        self.total_scraped = 0
        self.errors = 0
        
    def get_headers(self) -> Dict[str, str]:
        """Generate realistic browser headers"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8,de;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.mobile.de/',
            'Cache-Control': 'max-age=0',
        }
    
    def random_delay(self, min_sec: float = 2.0, max_sec: float = 5.0):
        """Random delay to avoid detection"""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a page with retries and exponential backoff"""
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    headers=self.get_headers(),
                    timeout=30
                )
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:  # Too many requests
                    wait_time = (2 ** attempt) * 10
                    print(f"⚠️  Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Status code {response.status_code} on attempt {attempt + 1}")
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Error on attempt {attempt + 1}: {str(e)}")
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) * 5
                    time.sleep(wait_time)
        
        self.errors += 1
        return None
    
    def inspect_html(self, html: str, save_to_file: bool = True):
        """Inspecciona el HTML para ayudar a encontrar selectores"""
        soup = BeautifulSoup(html, 'html.parser')
        
        print("\n" + "="*80)
        print("🔍 INSPECCIÓN DE HTML")
        print("="*80 + "\n")
        
        # Buscar diferentes patrones de divs que podrían contener coches
        possible_containers = [
            soup.find_all('div', class_=re.compile('.*result.*', re.I)),
            soup.find_all('div', class_=re.compile('.*listing.*', re.I)),
            soup.find_all('div', class_=re.compile('.*vehicle.*', re.I)),
            soup.find_all('div', class_=re.compile('.*ad.*', re.I)),
            soup.find_all('article'),
            soup.find_all('div', {'data-ad-id': True}),
            soup.find_all('div', {'data-vehicle-id': True}),
        ]
        
        print("Buscando contenedores de coches...")
        for i, containers in enumerate(possible_containers):
            if containers:
                print(f"  Patrón {i+1}: Encontrados {len(containers)} elementos")
                if containers:
                    # Mostrar clases del primer elemento
                    first = containers[0]
                    classes = first.get('class', [])
                    print(f"    Clases: {classes}")
        
        # Buscar enlaces que parezcan de coches
        all_links = soup.find_all('a', href=True)
        car_links = [a for a in all_links if '/fahrzeug/' in a['href'] or '/vehiculo/' in a['href']]
        print(f"\nEnlaces a coches encontrados: {len(car_links)}")
        
        if car_links:
            print(f"  Ejemplo: {car_links[0]['href'][:100]}")
        
        # Buscar precios
        price_patterns = [
            soup.find_all(string=re.compile(r'\d+[.,]\d+.*€')),
            soup.find_all('span', class_=re.compile('.*price.*', re.I)),
            soup.find_all('div', class_=re.compile('.*price.*', re.I)),
        ]
        
        print("\nPrecios encontrados:")
        for i, prices in enumerate(price_patterns):
            if prices:
                print(f"  Patrón {i+1}: {len(prices)} elementos")
                if prices:
                    print(f"    Ejemplo: {str(prices[0])[:100]}")
        
        # Guardar HTML para inspección manual
        if save_to_file:
            html_file = os.path.join(self.output_dir, 'sample_page.html')
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"\n💾 HTML guardado en: {html_file}")
            print("   Abre este archivo en tu navegador para inspeccionarlo")
        
        print("\n" + "="*80 + "\n")
    
    def extract_car_data_v2(self, car_element) -> Optional[Dict]:
        """Extracción mejorada con múltiples estrategias"""
        try:
            data = {}
            
            # Estrategia 1: Buscar título y enlace
            title_selectors = [
                car_element.find('h2'),
                car_element.find('h3'),
                car_element.find('a', class_=re.compile('.*title.*', re.I)),
                car_element.find('a', class_=re.compile('.*headline.*', re.I)),
            ]
            
            for title_elem in title_selectors:
                if title_elem:
                    if title_elem.name == 'a':
                        data['titulo'] = title_elem.get_text(strip=True)
                        data['url'] = title_elem.get('href', '')
                    else:
                        link = title_elem.find('a')
                        if link:
                            data['titulo'] = link.get_text(strip=True)
                            data['url'] = link.get('href', '')
                    if 'titulo' in data:
                        break
            
            # Asegurar URL completa
            if 'url' in data and data['url']:
                if not data['url'].startswith('http'):
                    data['url'] = 'https://www.mobile.de' + data['url']
            
            # Estrategia 2: Buscar precio
            price_text = None
            price_selectors = [
                car_element.find('span', class_=re.compile('.*price.*', re.I)),
                car_element.find('div', class_=re.compile('.*price.*', re.I)),
                car_element.find(string=re.compile(r'\d+\.\d+.*€')),
            ]
            
            for price_elem in price_selectors:
                if price_elem:
                    price_text = price_elem if isinstance(price_elem, str) else price_elem.get_text(strip=True)
                    if price_text and '€' in price_text:
                        break
            
            if price_text:
                # Limpiar precio: "24.990 €" -> 24990
                price_clean = re.sub(r'[^\d,.]', '', price_text)
                price_clean = price_clean.replace('.', '').replace(',', '.')
                try:
                    data['precio'] = float(price_clean)
                except:
                    pass
            
            # Estrategia 3: Buscar todos los textos y extraer datos
            all_text = car_element.get_text(separator='|', strip=True)
            
            # Kilometraje
            km_match = re.search(r'(\d+(?:\.\d+)?)\s*km', all_text, re.I)
            if km_match:
                try:
                    km_str = km_match.group(1).replace('.', '')
                    data['kilometros'] = int(km_str)
                except:
                    pass
            
            # Potencia en CV
            cv_patterns = [
                r'(\d+)\s*(?:CV|PS|ch)',  # 150 CV
                r'\((\d+)\s*CV\)',  # (150 CV)
                r'(\d+)\s*kW\s*\((\d+)\s*CV\)',  # 110 kW (150 CV)
            ]
            
            for pattern in cv_patterns:
                cv_match = re.search(pattern, all_text, re.I)
                if cv_match:
                    try:
                        # Si hay dos grupos, el segundo es el CV
                        cv_value = cv_match.group(2) if len(cv_match.groups()) > 1 and cv_match.group(2) else cv_match.group(1)
                        data['potencia_cv'] = int(cv_value)
                        break
                    except:
                        pass
            
            # Combustible
            fuel_keywords = ['Gasolina', 'Diesel', 'Diésel', 'Híbrido', 'Eléctrico', 'Benzin', 'Elektro']
            for keyword in fuel_keywords:
                if keyword.lower() in all_text.lower():
                    data['combustible'] = keyword
                    break
            
            # Primera matriculación (año)
            year_match = re.search(r'(?:^|[^\d])(20\d{2})(?:[^\d]|$)', all_text)
            if year_match:
                data['primera_matriculacion'] = year_match.group(1)
            
            # Ubicación
            location_patterns = [
                re.search(r'(?:DE|D|Alemania)[-\s](\d{5})\s+([A-Za-zäöüÄÖÜß\s]+)', all_text),
                re.search(r'(\d{5})\s+([A-Za-zäöüÄÖÜß\s]+)', all_text),
            ]
            
            for loc_match in location_patterns:
                if loc_match:
                    data['ubicacion'] = f"{loc_match.group(1)} {loc_match.group(2).strip()}"
                    break
            
            # Solo devolver si tenemos al menos título y precio
            if 'titulo' in data and 'precio' in data:
                return data
            
            return None
            
        except Exception as e:
            print(f"⚠️  Error extracting car data: {str(e)}")
            return None
    
    def scrape_page(self, html: str) -> List[Dict]:
        """Extract all cars from a page with multiple strategies"""
        cars = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Estrategia múltiple para encontrar listados
            car_elements = None
            
            # Intento 1: data-testid
            car_elements = soup.find_all('div', {'data-testid': 'result-item'})
            
            # Intento 2: clases comunes de mobile.de
            if not car_elements:
                car_elements = soup.find_all('div', class_=re.compile('.*cBox.*'))
            
            # Intento 3: article tags
            if not car_elements:
                car_elements = soup.find_all('article')
            
            # Intento 4: divs con data-ad-id
            if not car_elements:
                car_elements = soup.find_all('div', {'data-ad-id': True})
            
            # Intento 5: buscar por enlaces a vehículos
            if not car_elements:
                # Encontrar todos los enlaces a vehículos y subir al contenedor padre
                vehicle_links = soup.find_all('a', href=re.compile(r'/fahrzeug/|/vehiculo/'))
                parents = set()
                for link in vehicle_links:
                    # Subir hasta encontrar un div con suficiente contenido
                    parent = link.find_parent('div', class_=True)
                    if parent and len(parent.get_text(strip=True)) > 50:
                        parents.add(parent)
                car_elements = list(parents)
            
            print(f"   Found {len(car_elements)} potential car elements")
            
            if not car_elements:
                print("   ⚠️  No se encontraron elementos de coches. Inspeccionando HTML...")
                self.inspect_html(html)
                return []
            
            for car_elem in car_elements:
                car_data = self.extract_car_data_v2(car_elem)
                if car_data:
                    cars.append(car_data)
            
            print(f"   Successfully extracted {len(cars)} cars")
            
            # Si no se extrajo nada, inspeccionar el primer elemento
            if not cars and car_elements:
                print("\n   ⚠️  No se pudieron extraer datos. Inspeccionando primer elemento...")
                print("   " + "="*70)
                first_elem = car_elements[0]
                print(f"   Texto del elemento: {first_elem.get_text(separator=' | ', strip=True)[:200]}...")
                print("   " + "="*70)
            
        except Exception as e:
            print(f"❌ Error parsing page: {str(e)}")
        
        return cars
    
    def get_total_pages(self, html: str) -> int:
        """Extract total number of pages"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Buscar información de paginación
            pagination_patterns = [
                soup.find('span', string=re.compile(r'Página\s+\d+\s+de\s+(\d+)')),
                soup.find('div', class_=re.compile('.*pagination.*')),
                soup.find(string=re.compile(r'de\s+(\d+(?:\.\d+)?)\s+Ofertas', re.I)),
            ]
            
            for pattern in pagination_patterns:
                if pattern:
                    text = pattern if isinstance(pattern, str) else pattern.get_text()
                    # Extraer número de páginas
                    page_match = re.search(r'de\s+(\d+)', text, re.I)
                    if page_match:
                        total = int(page_match.group(1))
                        return total
            
            # Alternativa: buscar el número más alto en enlaces de página
            page_links = soup.find_all('a', class_=re.compile('.*page.*|.*pagination.*'))
            if page_links:
                numbers = []
                for link in page_links:
                    try:
                        num = int(link.get_text(strip=True))
                        numbers.append(num)
                    except:
                        pass
                if numbers:
                    return max(numbers)
            
            # Si no encontramos nada, asumir que hay múltiples páginas
            # y buscar en el HTML el número total de resultados
            results_match = re.search(r'(\d+(?:\.\d+)?)\s+Ofertas', html, re.I)
            if results_match:
                total_results = int(results_match.group(1).replace('.', ''))
                # Asumir 24 coches por página
                return (total_results // 24) + 1
            
            return 1
            
        except Exception as e:
            print(f"⚠️  Error getting total pages: {str(e)}")
            return 1
    
    def build_url(self, base_url: str, page: int) -> str:
        """Build URL for specific page"""
        parsed = urlparse(base_url)
        params = parse_qs(parsed.query)
        params['pageNumber'] = [str(page)]
        new_query = urlencode(params, doseq=True)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
    
    def save_checkpoint(self, data: List[Dict], year_range: str, page: int):
        """Save intermediate results"""
        if not data:
            return
        
        df = pd.DataFrame(data)
        checkpoint_file = os.path.join(
            self.output_dir,
            f"checkpoint_{year_range}_page_{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        df.to_csv(checkpoint_file, index=False, encoding='utf-8-sig')
        print(f"💾 Checkpoint saved: {len(data)} cars in {checkpoint_file}")
    
    def scrape_year_range(self, year_from: int, year_to: int, max_pages: Optional[int] = None):
        """Scrape cars for a specific year range"""
        print(f"\n{'='*60}")
        print(f"🚗 Scraping cars from {year_from} to {year_to}")
        print(f"{'='*60}\n")
        
        # Modify URL for year range
        parsed = urlparse(self.base_url)
        params = parse_qs(parsed.query)
        params['fr'] = [str(year_from)]
        params['to'] = [str(year_to)]
        
        new_query = urlencode(params, doseq=True)
        range_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
        
        print(f"🔗 URL: {range_url}\n")
        
        # Fetch first page
        print("📊 Fetching first page...")
        html = self.fetch_page(range_url)
        
        if not html:
            print("❌ Failed to fetch first page")
            return []
        
        # Inspeccionar HTML de la primera página
        print("🔍 Inspecting first page structure...")
        total_pages = self.get_total_pages(html)
        print(f"📄 Total pages detected: {total_pages}")
        
        if max_pages:
            total_pages = min(total_pages, max_pages)
            print(f"   (Limited to {max_pages} pages)")
        
        all_cars = []
        year_range_str = f"{year_from}_{year_to}"
        
        # Scrape each page
        for page in range(1, total_pages + 1):
            print(f"\n📖 Page {page}/{total_pages}")
            
            if page == 1:
                page_html = html
            else:
                page_url = self.build_url(range_url, page)
                page_html = self.fetch_page(page_url)
            
            if not page_html:
                print(f"⚠️  Skipping page {page}")
                continue
            
            # Extract cars from page
            cars = self.scrape_page(page_html)
            all_cars.extend(cars)
            self.total_scraped += len(cars)
            
            print(f"   ✅ Extracted {len(cars)} cars (Total: {len(all_cars)})")
            
            # Save checkpoint every 100 cars
            if len(all_cars) >= 100 and len(all_cars) % 100 < 50:
                self.save_checkpoint(all_cars, year_range_str, page)
            
            # Random delay
            if page < total_pages:
                self.random_delay(2.0, 5.0)
        
        # Save final results
        if all_cars:
            df = pd.DataFrame(all_cars)
            final_file = os.path.join(
                self.output_dir,
                f"mobile_de_{year_range_str}_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            df.to_csv(final_file, index=False, encoding='utf-8-sig')
            print(f"\n✅ Final file saved: {final_file}")
            print(f"   Total cars: {len(all_cars)}")
            
            # Mostrar estadísticas
            print(f"\n📊 Estadísticas:")
            print(f"   Coches con potencia: {df['potencia_cv'].notna().sum()}/{len(df)}")
            print(f"   Coches con kilometraje: {df['kilometros'].notna().sum()}/{len(df)}")
        else:
            print("\n⚠️  No se extrajeron coches de este rango")
        
        return all_cars
    
    def scrape_all_years(self, year_ranges: List[tuple], max_pages_per_range: Optional[int] = None):
        """Scrape all year ranges"""
        print(f"\n{'='*60}")
        print(f"🚀 MOBILE.DE SCRAPER V2")
        print(f"{'='*60}")
        print(f"Year ranges: {year_ranges}")
        print(f"Output directory: {self.output_dir}")
        print(f"{'='*60}\n")
        
        start_time = datetime.now()
        all_data = []
        
        for year_from, year_to in year_ranges:
            cars = self.scrape_year_range(year_from, year_to, max_pages_per_range)
            all_data.extend(cars)
            
            if (year_from, year_to) != year_ranges[-1]:
                print("\n⏸️  Taking a break between year ranges...")
                time.sleep(10)
        
        # Save combined file
        if all_data:
            df = pd.DataFrame(all_data)
            combined_file = os.path.join(
                self.output_dir,
                f"mobile_de_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            df.to_csv(combined_file, index=False, encoding='utf-8-sig')
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n{'='*60}")
            print(f"✅ SCRAPING COMPLETED!")
            print(f"{'='*60}")
            print(f"Total cars scraped: {len(all_data)}")
            print(f"Total errors: {self.errors}")
            print(f"Duration: {duration}")
            print(f"Combined file: {combined_file}")
            print(f"{'='*60}\n")
            
            print("\n📊 Dataset summary:")
            for col in df.columns:
                non_null = df[col].notna().sum()
                print(f"   {col}: {non_null}/{len(df)} ({non_null/len(df)*100:.1f}%)")
        else:
            print("\n❌ No se pudieron extraer datos. Revisa el archivo sample_page.html")
        
        return all_data


def main():
    base_url = "https://www.mobile.de/es/veh%C3%ADculos/buscar.html?isSearchRequest=true&s=Car&vc=Car&p=%3A30000&fr=2013&ml=%3A150000&cn=DE&pw=110&emc=EURO6&sr=4&ft=PETROL&ft=DIESEL&st=DEALER&ref=dsp"
    
    year_ranges = [
        (2013, 2015),
        (2016, 2018),
        (2019, 2021),
        (2022, 2025)
    ]
    
    scraper = MobileDeScraper(base_url)
    
    print("🔧 MODO: Testing (primeras 2 páginas de primer rango)")
    print("   Si funciona, cambia a scraping completo\n")
    
    # TEST MODE: Solo 2 páginas del primer rango
    scraper.scrape_year_range(year_ranges[0][0], year_ranges[0][1], max_pages=2)
    
    # Para scraping completo, descomenta:
    # scraper.scrape_all_years(year_ranges)


if __name__ == "__main__":
    main()