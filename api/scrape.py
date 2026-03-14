"""
Vercel Serverless Function: POST /api/scrape

Request body:
  { "url": "https://...", "max_reviews": 30 }

Response:
  { "reviews": [...], "product_name": "...", "error": null }
"""

import json
import re
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler

import requests
from bs4 import BeautifulSoup

# ブラウザに偽装するヘッダー
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_DELAY = 1.5  # ページ間の待機秒数


# ──────────────────────────────────────────
# URL解析
# ──────────────────────────────────────────

def detect_mall(url: str) -> str | None:
    if "amazon.co.jp" in url:
        return "amazon"
    if "rakuten.co.jp" in url:
        return "rakuten"
    return None


def extract_asin(url: str) -> str | None:
    for pattern in [r"/dp/([A-Z0-9]{10})", r"/product-reviews/([A-Z0-9]{10})", r"/gp/product/([A-Z0-9]{10})"]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return None


def extract_rakuten_ids(url: str) -> tuple[str, str] | tuple[None, None]:
    m = re.search(r"item\.rakuten\.co\.jp/([^/?#]+)/([^/?#]+)", url)
    if m:
        return m.group(1), m.group(2).rstrip("/")
    m = re.search(r"review\.rakuten\.co\.jp/item/1/([^/]+)/([^/]+)", url)
    if m:
        return m.group(1), m.group(2)
    return None, None


# ──────────────────────────────────────────
# Amazon スクレイパー
# ──────────────────────────────────────────

def scrape_amazon(url: str, max_reviews: int) -> dict:
    asin = extract_asin(url)
    if not asin:
        return {"error": "ASINを取得できませんでした。URLを確認してください。", "reviews": [], "product_name": ""}

    session = requests.Session()
    session.headers.update(HEADERS)

    # 商品名を取得
    product_name = _amazon_product_name(session, asin)

    reviews = []
    for page_num in range(1, 20):
        if len(reviews) >= max_reviews:
            break

        review_url = (
            f"https://www.amazon.co.jp/product-reviews/{asin}/"
            f"?pageNumber={page_num}&sortBy=recent"
        )
        try:
            resp = session.get(review_url, timeout=12)
        except requests.RequestException as e:
            return {"error": f"ネットワークエラー: {e}", "reviews": reviews, "product_name": product_name}

        if resp.status_code != 200:
            break

        # ブロック検知
        if "captcha" in resp.url or "ap/signin" in resp.url:
            return {
                "error": "Amazonのアクセス制限（CAPTCHA）が発生しました。時間をおいて再試行してください。",
                "reviews": reviews,
                "product_name": product_name,
            }

        soup = BeautifulSoup(resp.text, "lxml")
        page_reviews = _parse_amazon_reviews(soup, asin, product_name)

        if not page_reviews:
            break

        reviews.extend(page_reviews)
        time.sleep(REQUEST_DELAY)

    return {"error": None, "reviews": reviews[:max_reviews], "product_name": product_name}


def _amazon_product_name(session: requests.Session, asin: str) -> str:
    try:
        resp = session.get(f"https://www.amazon.co.jp/dp/{asin}", timeout=10)
        soup = BeautifulSoup(resp.text, "lxml")
        el = soup.select_one("#productTitle")
        if el:
            return el.get_text(strip=True)
    except Exception:
        pass
    return asin


def _parse_amazon_reviews(soup: BeautifulSoup, asin: str, product_name: str) -> list[dict]:
    reviews = []
    for div in soup.select("[data-hook='review']"):
        try:
            # 評価
            rating = None
            rating_el = div.select_one("[data-hook='review-star-rating'] .a-icon-alt")
            if rating_el:
                m = re.search(r"([\d.]+)", rating_el.get_text())
                if m:
                    rating = float(m.group(1))

            # タイトル
            title = ""
            title_el = div.select_one("[data-hook='review-title']")
            if title_el:
                # アイコンのテキストを除く
                for icon in title_el.select(".a-icon-alt"):
                    icon.decompose()
                title = title_el.get_text(strip=True)

            # 本文
            body = ""
            body_el = div.select_one("[data-hook='review-body'] span")
            if body_el:
                body = body_el.get_text(strip=True)

            # 投稿日
            review_date = ""
            date_el = div.select_one("[data-hook='review-date']")
            if date_el:
                m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_el.get_text())
                if m:
                    review_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

            # 役立った数
            helpful_count = 0
            helpful_el = div.select_one("[data-hook='helpful-vote-statement']")
            if helpful_el:
                m = re.search(r"(\d+)", helpful_el.get_text())
                if m:
                    helpful_count = int(m.group(1))

            # バリエーション
            variant = ""
            variant_el = div.select_one("[data-hook='format-strip']")
            if variant_el:
                variant = variant_el.get_text(strip=True)

            if body:
                reviews.append({
                    "モール": "Amazon",
                    "商品名": product_name,
                    "ASIN": asin,
                    "評価": rating,
                    "タイトル": title,
                    "本文": body,
                    "投稿日": review_date,
                    "役立った数": helpful_count,
                    "バリエーション": variant,
                    "収集日": datetime.now().strftime("%Y-%m-%d"),
                })
        except Exception:
            continue
    return reviews


# ──────────────────────────────────────────
# 楽天市場 スクレイパー
# ──────────────────────────────────────────

def scrape_rakuten(url: str, max_reviews: int) -> dict:
    shop_id, item_id = extract_rakuten_ids(url)
    if not shop_id or not item_id:
        return {"error": "ショップID・商品IDを取得できませんでした。URLを確認してください。", "reviews": [], "product_name": ""}

    session = requests.Session()
    session.headers.update(HEADERS)

    product_name = _rakuten_product_name(session, shop_id, item_id, url)

    reviews = []
    for page_num in range(1, 30):
        if len(reviews) >= max_reviews:
            break

        review_url = f"https://review.rakuten.co.jp/item/1/{shop_id}/{item_id}/{page_num}/"
        try:
            resp = session.get(review_url, timeout=12)
        except requests.RequestException as e:
            return {"error": f"ネットワークエラー: {e}", "reviews": reviews, "product_name": product_name}

        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "lxml")
        page_reviews = _parse_rakuten_reviews(soup, shop_id, item_id, product_name)

        if not page_reviews:
            # レビューなし or 最終ページ
            break

        reviews.extend(page_reviews)

        # 次ページリンク確認
        if not soup.select_one("a[rel='next'], .paginationNext a, [class*='next'] a"):
            break

        time.sleep(REQUEST_DELAY)

    return {"error": None, "reviews": reviews[:max_reviews], "product_name": product_name}


def _rakuten_product_name(session: requests.Session, shop_id: str, item_id: str, original_url: str) -> str:
    # 商品ページから取得
    if "item.rakuten.co.jp" in original_url:
        try:
            resp = session.get(original_url, timeout=10)
            soup = BeautifulSoup(resp.text, "lxml")
            for selector in [".item_name", "h1.item-name", "h1", "title"]:
                el = soup.select_one(selector)
                if el:
                    name = el.get_text(strip=True)
                    if name and len(name) > 3:
                        return name[:100]
        except Exception:
            pass

    # レビューページから取得
    try:
        review_url = f"https://review.rakuten.co.jp/item/1/{shop_id}/{item_id}/1/"
        resp = session.get(review_url, timeout=10)
        soup = BeautifulSoup(resp.text, "lxml")
        for selector in [".revItemBox__itemName", ".item-name", "h1"]:
            el = soup.select_one(selector)
            if el:
                name = el.get_text(strip=True)
                if name and len(name) > 3:
                    return name[:100]
    except Exception:
        pass

    return f"{shop_id}/{item_id}"


def _parse_rakuten_reviews(soup: BeautifulSoup, shop_id: str, item_id: str, product_name: str) -> list[dict]:
    reviews = []

    # 複数のセレクタパターンを試みる（楽天はクラス名が変わることがある）
    containers = (
        soup.select(".revRvwUserRevBox")
        or soup.select(".review-item")
        or soup.select("[class*='reviewBox']")
        or soup.select("[class*='revItem']")
    )

    for container in containers:
        try:
            # 評価
            rating = _extract_rakuten_rating(container)

            # タイトル
            title = ""
            for sel in [".revRvwUserRevTtl", "[class*='title']", "[class*='Title']"]:
                el = container.select_one(sel)
                if el:
                    title = el.get_text(strip=True)
                    break

            # 本文
            body = ""
            for sel in [".revRvwUserRevBody", "[class*='body']", "[class*='Body']", "[class*='comment']", "p"]:
                el = container.select_one(sel)
                if el:
                    text = el.get_text(strip=True)
                    if len(text) > 10:  # タイトルと区別するため短すぎるものは除外
                        body = text
                        break

            # 投稿日
            review_date = ""
            for sel in [".revRvwUserRevDate", "[class*='date']", "[class*='Date']", "time"]:
                el = container.select_one(sel)
                if el:
                    date_text = el.get("datetime") or el.get_text()
                    m = re.search(r"(\d{4})[-年/](\d{1,2})[-月/](\d{1,2})", date_text)
                    if m:
                        review_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                        break

            # 購入者情報
            user_info = ""
            for sel in ["[class*='userInfo']", "[class*='userData']", "[class*='reviewer']"]:
                el = container.select_one(sel)
                if el:
                    user_info = el.get_text(strip=True)
                    break

            if body:
                reviews.append({
                    "モール": "楽天",
                    "商品名": product_name,
                    "shop_id": shop_id,
                    "item_id": item_id,
                    "評価": rating,
                    "タイトル": title,
                    "本文": body,
                    "投稿日": review_date,
                    "投稿者情報": user_info,
                    "収集日": datetime.now().strftime("%Y-%m-%d"),
                })
        except Exception:
            continue

    return reviews


def _extract_rakuten_rating(container: BeautifulSoup) -> float | None:
    for sel in ["[class*='rating']", "[class*='star']", "[class*='Rating']", "[class*='Star']"]:
        el = container.select_one(sel)
        if not el:
            continue
        # class名から数値を取る (例: "star5", "rating-4")
        class_str = " ".join(el.get("class", []))
        m = re.search(r"[^0-9]([1-5])[^0-9]?$", class_str) or re.search(r"[^0-9]([1-5])[^0-9]", class_str)
        if m:
            return float(m.group(1))
        # aria-label / title 属性
        for attr in ["aria-label", "title"]:
            val = el.get(attr, "")
            m = re.search(r"([\d.]+)", val)
            if m:
                v = float(m.group(1))
                if 1.0 <= v <= 5.0:
                    return v
    return None


# ──────────────────────────────────────────
# Vercel ハンドラ
# ──────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # サーバーログを抑制

    def do_OPTIONS(self):
        self._send_cors()

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except (json.JSONDecodeError, ValueError):
            self._respond(400, {"error": "リクエストのJSON形式が不正です。"})
            return

        url = (body.get("url") or "").strip()
        max_reviews = min(int(body.get("max_reviews", 30)), 100)

        if not url:
            self._respond(400, {"error": "URLが指定されていません。"})
            return

        mall = detect_mall(url)
        if mall == "amazon":
            result = scrape_amazon(url, max_reviews)
        elif mall == "rakuten":
            result = scrape_rakuten(url, max_reviews)
        else:
            self._respond(400, {"error": "対応していないURLです。Amazon・楽天のURLを指定してください。"})
            return

        self._respond(200, result)

    def _respond(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_cors(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
