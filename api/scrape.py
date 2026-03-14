"""
Vercel Serverless Function: POST /api/scrape

Request body:
  { "url": "https://...", "max_reviews": 30 }

Response:
  { "reviews": [...], "product_name": "...", "error": null, "debug": {...} }
"""

import json
import re
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler

import requests
from bs4 import BeautifulSoup

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

REQUEST_DELAY = 1.5


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


def extract_rakuten_ids(url: str):
    m = re.search(r"item\.rakuten\.co\.jp/([^/?#]+)/([^/?#]+)", url)
    if m:
        return m.group(1), m.group(2).rstrip("/")
    m = re.search(r"review\.rakuten\.co\.jp/item/1/([^/]+)/([^/]+)", url)
    if m:
        return m.group(1), m.group(2)
    return None, None


# ──────────────────────────────────────────
# セッション作成
# ──────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def safe_get(session: requests.Session, url: str, timeout: int = 15):
    """GETリクエスト。失敗時はNoneを返す。"""
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        return resp
    except requests.RequestException as e:
        return None


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ──────────────────────────────────────────
# Amazon スクレイパー
# ──────────────────────────────────────────

def scrape_amazon(url: str, max_reviews: int) -> dict:
    asin = extract_asin(url)
    if not asin:
        return _result([], "", "ASINを取得できませんでした。URLを確認してください。")

    session = make_session()
    # Amazon.co.jpのトップを先に訪問してCookieを取得
    safe_get(session, "https://www.amazon.co.jp/", timeout=10)
    time.sleep(1)

    product_name = _amazon_product_name(session, asin)
    reviews = []
    debug_info = []

    for page_num in range(1, 15):
        if len(reviews) >= max_reviews:
            break

        review_url = (
            f"https://www.amazon.co.jp/product-reviews/{asin}/"
            f"?pageNumber={page_num}&sortBy=recent&reviewerType=all_reviews"
        )
        resp = safe_get(session, review_url)
        if resp is None:
            return _result(reviews, product_name, "ネットワークエラーが発生しました。", debug_info)

        # ブロック検知
        if "captcha" in resp.url or "ap/signin" in resp.url or resp.status_code in (403, 503):
            return _result(
                reviews, product_name,
                f"Amazonのアクセス制限が発生しました（ステータス: {resp.status_code}）。時間をおいて再試行してください。",
                debug_info,
            )

        soup = parse_html(resp.text)
        page_title = soup.title.string if soup.title else "（タイトルなし）"
        containers = soup.select("[data-hook='review']")
        debug_info.append({
            "page": page_num,
            "url": review_url,
            "status": resp.status_code,
            "title": page_title,
            "containers_found": len(containers),
        })

        if not containers:
            # ページにレビューがない = 最終ページ or ブロック
            break

        page_reviews = _parse_amazon_reviews(soup, asin, product_name)
        reviews.extend(page_reviews)
        time.sleep(REQUEST_DELAY)

    error = None if reviews else "レビューを取得できませんでした。Amazonのbot対策により制限された可能性があります。"
    return _result(reviews[:max_reviews], product_name, error, debug_info)


def _amazon_product_name(session: requests.Session, asin: str) -> str:
    resp = safe_get(session, f"https://www.amazon.co.jp/dp/{asin}")
    if not resp:
        return asin
    soup = parse_html(resp.text)
    el = soup.select_one("#productTitle")
    return el.get_text(strip=True) if el else asin


def _parse_amazon_reviews(soup: BeautifulSoup, asin: str, product_name: str) -> list:
    reviews = []
    today = datetime.now().strftime("%Y-%m-%d")
    for div in soup.select("[data-hook='review']"):
        try:
            # 評価
            rating = None
            r_el = div.select_one("[data-hook='review-star-rating'] .a-icon-alt")
            if r_el:
                m = re.search(r"([\d.]+)", r_el.get_text())
                if m:
                    rating = float(m.group(1))

            # タイトル（アイコンを除去）
            title = ""
            t_el = div.select_one("[data-hook='review-title']")
            if t_el:
                for icon in t_el.select(".a-icon-alt"):
                    icon.decompose()
                title = t_el.get_text(strip=True)

            # 本文
            body = ""
            b_el = div.select_one("[data-hook='review-body'] span")
            if b_el:
                body = b_el.get_text(strip=True)

            # 投稿日
            review_date = ""
            d_el = div.select_one("[data-hook='review-date']")
            if d_el:
                m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", d_el.get_text())
                if m:
                    review_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

            # 役立った数
            helpful = 0
            h_el = div.select_one("[data-hook='helpful-vote-statement']")
            if h_el:
                m = re.search(r"(\d+)", h_el.get_text())
                if m:
                    helpful = int(m.group(1))

            if body:
                reviews.append({
                    "モール": "Amazon",
                    "商品名": product_name,
                    "ASIN": asin,
                    "評価": rating,
                    "タイトル": title,
                    "本文": body,
                    "投稿日": review_date,
                    "役立った数": helpful,
                    "収集日": today,
                })
        except Exception:
            continue
    return reviews


# ──────────────────────────────────────────
# 楽天市場 スクレイパー
# ──────────────────────────────────────────

def scrape_rakuten(url: str, max_reviews: int) -> dict:
    session = make_session()
    safe_get(session, "https://www.rakuten.co.jp/", timeout=10)
    time.sleep(1)

    # review.rakuten.co.jp URL はそのまま使う
    # item.rakuten.co.jp URL はレビューページURLに変換する
    if "review.rakuten.co.jp" in url:
        start_url = url.rstrip("/") + "/"
    else:
        shop_id, item_id = extract_rakuten_ids(url)
        if not shop_id or not item_id:
            return _result([], "", "ショップID・商品IDを取得できませんでした。URLを確認してください。")
        start_url = f"https://review.rakuten.co.jp/item/1/{shop_id}/{item_id}/1.1/"

    reviews = []
    debug_info = []
    current_url = start_url
    product_name = ""
    page_num = 0

    while len(reviews) < max_reviews:
        page_num += 1
        resp = safe_get(session, current_url)
        if resp is None:
            return _result(reviews, product_name, "ネットワークエラーが発生しました。", debug_info)

        soup = parse_html(resp.text)
        page_title = soup.title.string if soup.title else "（タイトルなし）"

        # 商品名（初回のみ取得）
        if not product_name:
            product_name = _rakuten_product_name_from_review_page(soup, url, session)

        # レビューコンテナを広く探す
        containers = (
            soup.select(".revRvwUserRevBox")
            or soup.select("[class*='revRvwUserRev']")
            or soup.select("[class*='reviewItem']")
            or soup.select("[class*='review-item']")
            or soup.select("[class*='ReviewItem']")
        )

        debug_info.append({
            "page": page_num,
            "url": current_url,
            "status": resp.status_code,
            "title": page_title,
            "containers_found": len(containers),
        })

        if not containers:
            break

        page_reviews = _parse_rakuten_reviews(soup, containers, product_name)
        reviews.extend(page_reviews)

        # 次ページリンクを探してたどる（URL構築はしない）
        next_url = _find_next_page_url(soup, current_url)
        if not next_url:
            break
        current_url = next_url
        time.sleep(REQUEST_DELAY)

    error = None if reviews else "レビューを取得できませんでした。ページ構造が変更されたか、レビューが0件の可能性があります。"
    return _result(reviews[:max_reviews], product_name, error, debug_info)


def _rakuten_product_name_from_review_page(soup: BeautifulSoup, original_url: str, session: requests.Session) -> str:
    # レビューページ内の商品名要素を探す
    for sel in [".revItemBox__itemName", ".item-name", "[class*='itemName']", "[class*='item_name']"]:
        el = soup.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if len(name) > 3:
                return name[:120]

    # item.rakuten.co.jp URLなら商品ページから取得
    if "item.rakuten.co.jp" in original_url:
        resp = safe_get(session, original_url)
        if resp:
            item_soup = parse_html(resp.text)
            for sel in [".item_name", "h1.item-name", "#rakutenLimitedId_title", "h1"]:
                el = item_soup.select_one(sel)
                if el:
                    name = el.get_text(strip=True)
                    if len(name) > 3:
                        return name[:120]

    return ""


def _find_next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    """ページ内の「次へ」リンクを探してフルURLを返す"""
    # rel="next" が最も信頼できる
    el = soup.select_one("a[rel='next']")
    if el and el.get("href"):
        return _abs_url(el["href"], current_url)

    # テキスト「次」を含むページネーションリンク
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        cls = " ".join(a.get("class", []))
        if text in ("次へ", "次", ">", "»") or "next" in cls.lower():
            return _abs_url(a["href"], current_url)

    return None


def _abs_url(href: str, base_url: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    # 相対URL
    return base_url.rsplit("/", 1)[0] + "/" + href


def _parse_rakuten_reviews(soup: BeautifulSoup, containers, product_name: str) -> list:
    reviews = []
    today = datetime.now().strftime("%Y-%m-%d")
    for container in containers:
        try:
            # 評価
            rating = _extract_rakuten_rating(container)

            # タイトル
            title = ""
            for sel in [".revRvwUserRevTtl", "[class*='title']", "[class*='Title']", "h3", "h4"]:
                el = container.select_one(sel)
                if el:
                    title = el.get_text(strip=True)
                    break

            # 本文 ― テキストが最も長い <p> または div を本文と見なす
            body = ""
            candidates = container.select("p, [class*='body'], [class*='Body'], [class*='comment'], [class*='text']")
            for el in candidates:
                text = el.get_text(strip=True)
                if len(text) > len(body):
                    body = text

            # 本文がまだ取れていなければ最も長いテキストノードを使う
            if not body:
                all_text = [el.get_text(strip=True) for el in container.find_all(True)]
                if all_text:
                    body = max(all_text, key=len)

            # 投稿日
            review_date = ""
            for sel in ["[class*='date']", "[class*='Date']", "time"]:
                el = container.select_one(sel)
                if el:
                    raw = el.get("datetime") or el.get_text()
                    m = re.search(r"(\d{4})[-年/](\d{1,2})[-月/](\d{1,2})", raw)
                    if m:
                        review_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                        break

            if body and len(body) > 5:
                reviews.append({
                    "モール": "楽天",
                    "商品名": product_name,
                    "評価": rating,
                    "タイトル": title,
                    "本文": body,
                    "投稿日": review_date,
                    "収集日": today,
                })
        except Exception:
            continue
    return reviews


def _extract_rakuten_rating(container) -> float | None:
    for sel in ["[class*='rating']", "[class*='star']", "[class*='Rating']", "[class*='Star']"]:
        el = container.select_one(sel)
        if not el:
            continue
        class_str = " ".join(el.get("class", []))
        # class名末尾の数字（例: star5, rating-4）
        m = re.search(r"[^0-9]([1-5])(?:[^0-9]|$)", class_str)
        if m:
            return float(m.group(1))
        for attr in ["aria-label", "title"]:
            val = el.get(attr, "")
            m = re.search(r"([\d.]+)", val)
            if m:
                v = float(m.group(1))
                if 1.0 <= v <= 5.0:
                    return v
    return None


# ──────────────────────────────────────────
# ヘルパー
# ──────────────────────────────────────────

def _result(reviews: list, product_name: str, error=None, debug=None) -> dict:
    return {
        "reviews": reviews,
        "product_name": product_name,
        "error": error,
        "debug": debug or [],
    }


# ──────────────────────────────────────────
# Vercel ハンドラ
# ──────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except (json.JSONDecodeError, ValueError):
            self._respond(400, _result([], "", "リクエストのJSON形式が不正です。"))
            return

        url = (body.get("url") or "").strip()
        max_reviews = min(int(body.get("max_reviews", 30)), 100)

        if not url:
            self._respond(400, _result([], "", "URLが指定されていません。"))
            return

        mall = detect_mall(url)
        if mall == "amazon":
            result = scrape_amazon(url, max_reviews)
        elif mall == "rakuten":
            result = scrape_rakuten(url, max_reviews)
        else:
            self._respond(400, _result([], "", "対応していないURLです。Amazon・楽天のURLを指定してください。"))
            return

        self._respond(200, result)

    def _respond(self, status: int, data: dict):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
