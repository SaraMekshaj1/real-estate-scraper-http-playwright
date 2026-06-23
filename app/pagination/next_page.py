from __future__ import annotations
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class NextPage:
    @staticmethod
    def get_next_page(html: str, url: str) -> str | None:
        soup     = BeautifulSoup(html, "html.parser")
        next_btn = soup.select_one("a[rel='next']")
        if next_btn:
            return urljoin(url, next_btn.get("href"))
        return None