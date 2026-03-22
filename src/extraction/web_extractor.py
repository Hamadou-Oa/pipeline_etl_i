"""
Extraction professionnelle des données depuis books.toscrape.com
- Scraping multi-pages
- Gestion des erreurs et retry
- Compatible avec le reste du pipeline (classe + méthode extract())
"""

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = "https://books.toscrape.com/catalogue/"


class WebExtractor:
    """
    Extrait les livres depuis books.toscrape.com .
    """

    def __init__(self, base_url: str = BASE_URL, max_pages: int = 5, delay: float = 0.5):
        """
        Args:
            base_url  : URL racine du site
            max_pages : Nombre max de pages à scraper 50 = site complet
            delay     : Délai entre requêtes 
        """
        self.base_url = base_url
        self.max_pages = max_pages
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (ETL Pipeline - Africa tech up Tour Project)"
        })

    def _get_page(self, url: str) -> BeautifulSoup:
        """Récupère et parse une page HTML."""
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _parse_books(self, soup: BeautifulSoup) -> list[dict]:
        """Extrait les livres d'une page."""
        books = []
        for article in soup.select("article.product_pod"):
            try:
                title = article.h3.a["title"]
                raw_price = article.select_one(".price_color").text
                price_clean = float(re.sub(r"[^\d.]", "", raw_price))
                availability = article.select_one(".availability").text.strip()
                rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
                rating_word = article.select_one("p.star-rating")["class"][1]
                rating = rating_map.get(rating_word, 0)

                books.append({
                    "title": title,
                    "price": price_clean,
                    "availability": availability,
                    "rating": rating,
                    "source": "web"
                })
            except Exception as e:
                logger.warning(f"Erreur parsing livre : {e}")
                continue
        return books

    def _get_next_page_url(self, soup: BeautifulSoup, current_page: int) -> str | None:
        next_btn = soup.select_one("li.next a")
        if not next_btn:
            return None
        if current_page == 1:
            return f"{CATALOGUE_URL}{next_btn['href']}"
        return f"{CATALOGUE_URL}{next_btn['href']}"

    def extract(self) -> pd.DataFrame:
        """
        scrape de toutes les pages jusqu'à max_pages.
        Returns:
            pd.DataFrame avec colonnes : title, price, availability, rating, source
        """
        all_books = []
        current_url = self.base_url
        page = 1

        logger.info(f"Début du scraping (max {self.max_pages} pages)")

        while current_url and page <= self.max_pages:
            try:
                logger.info(f"Scraping page {page} : {current_url}")
                soup = self._get_page(current_url)
                books = self._parse_books(soup)
                all_books.extend(books)
                logger.info(f"  → {len(books)} livres extraits")

                current_url = self._get_next_page_url(soup, page)
                page += 1
                time.sleep(self.delay)

            except requests.RequestException as e:
                logger.error(f"Erreur réseau page {page} : {e}")
                break

        df = pd.DataFrame(all_books)
        logger.info(f"Scraping terminé : {len(df)} livres au total")
        return df


if __name__ == "__main__":
    extractor = WebExtractor(max_pages=3)
    df = extractor.extract()
    print(df.head())
    print(f"\nTotal : {len(df)} livres")
