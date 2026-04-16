import html
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
import streamlit as st

MISSING = object()

API_KEY_ENV = "FLYBY_API_KEY"
API_BASE_URL = os.getenv(
    "FLYBY_API_BASE_URL", "https://real-time-amazon-data-the-most-complete.p.rapidapi.com"
)
API_HOST = os.getenv(
    "FLYBY_API_HOST", "real-time-amazon-data-the-most-complete.p.rapidapi.com"
)
FAVORITES_FILE = Path(__file__).resolve().parent / "favorites.json"
NEW_RELEASE_HISTORY_FILE = Path(__file__).resolve().parent / "new_releases_history.json"
HISTORY_RETENTION_DAYS = 90
RANK_CHANGE_LOOKBACK_DAYS = 3

FIXED_NEW_RELEASE_URLS = [
    "https://www.amazon.co.uk/gp/new-releases/automotive/ref=zg_bsnr_nav_automotive_0",
    "https://www.amazon.co.uk/gp/newreleases/automotive/301308031/ref=zg_bsnr_unv_automotive_2_2481713031_2",
    "https://www.amazon.co.uk/gp/new-releases/baby/ref=zg_bsnr_pg_2_baby?ie=UTF8&pg=2",
    "https://www.amazon.co.uk/gp/new-releases/beauty/ref=zg_bsnr_nav_beauty_0",
    "https://www.amazon.co.uk/gp/new-releases/diy/ref=zg_bsnr_unv_diy_1_1938722031_1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/ref=zg_bsnr_nav_outdoors_0",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/11714771/ref=zg_bsnr_nav_outdoors_1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/451114031/ref=zg_bsnr_nav_outdoors_1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/27917239031/ref=zg_bsnr_nav_outdoors_1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/4262907031/ref=zg_bsnr_nav_outdoors_2_27917239031",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/4262903031/ref=zg_bsnr_pg_1_outdoors?ie=UTF8&pg=1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/26389565031/ref=zg_bsnr_nav_outdoors_1",
    "https://www.amazon.co.uk/gp/new-releases/outdoors/4224744031/ref=zg_bsnr_nav_outdoors_1",
    "https://www.amazon.co.uk/gp/new-releases/drugstore/ref=zg_bsnr_nav_drugstore_0",
    "https://www.amazon.co.uk/gp/new-releases/kitchen/ref=zg_bsnr_nav_kitchen_0",
    "https://www.amazon.co.uk/gp/new-releases/kitchen/392546011/ref=zg_bsnr_nav_kitchen_1",
    "https://www.amazon.co.uk/gp/new-releases/kitchen/376320011/ref=zg_bsnr_pg_2_kitchen?ie=UTF8&pg=2",
    "https://www.amazon.co.uk/gp/new-releases/lighting/ref=zg_bsnr_nav_lighting_0",
    "https://www.amazon.co.uk/gp/new-releases/pet-supplies/ref=zg_bsnr_nav_pet-supplies_0",
    "https://www.amazon.co.uk/gp/new-releases/pet-supplies/451110031/ref=zg_bsnr_nav_pet-supplies_1",
    "https://www.amazon.co.uk/gp/new-releases/pet-supplies/451109031/ref=zg_bsnr_unv_pet-supplies_2_471284031_1",
    "https://www.amazon.co.uk/gp/new-releases/sports/ref=zg_bsnr_nav_sports_0",
    "https://www.amazon.co.uk/gp/new-releases/sports/324078011/ref=zg_bsnr_nav_sports_1",
    "https://www.amazon.co.uk/gp/new-releases/officeproduct/ref=zg_bsnr_nav_officeproduct_0",
    "https://www.amazon.co.uk/gp/new-releases/kids/ref=zg_bsnr_nav_kids_0",
    "https://www.amazon.co.uk/gp/new-releases/kids/364046031/ref=zg_bsnr_nav_kids_1",
    "https://www.amazon.co.uk/gp/new-releases/kids/364272031/ref=zg_bsnr_pg_2_kids?ie=UTF8&pg=2",
    "https://www.amazon.co.uk/gp/new-releases/kids/364234031/ref=zg_bsnr_nav_kids_1",
    "https://www.amazon.co.uk/gp/new-releases/kids/14520066031/ref=zg_bsnr_pg_2_kids?ie=UTF8&pg=2",
]

FIXED_MOVERS_URLS = [
    "https://www.amazon.co.uk/gp/movers-and-shakers/automotive/ref=zg_bsms_nav_automotive_0_amazon-renewed",
    "https://www.amazon.co.uk/gp/movers-and-shakers/baby/ref=zg_bsms_nav_baby_0_automotive",
    "https://www.amazon.co.uk/gp/movers-and-shakers/outdoors/ref=zg_bsms_nav_outdoors_0_diy",
    "https://www.amazon.co.uk/gp/movers-and-shakers/handmade/ref=zg_bsms_nav_handmade_0_grocery",
    "https://www.amazon.co.uk/gp/movers-and-shakers/kitchen/ref=zg_bsms_nav_kitchen_0_drugstore",
    "https://www.amazon.co.uk/gp/movers-and-shakers/lighting/ref=zg_bsms_nav_lighting_0_appliances",
]


@dataclass(frozen=True)
class CollectionTask:
    endpoint: str
    marketplace: str
    category: str = ""
    category_node: str = ""
    page: int = 1
    source_url: str = ""


def parse_keywords(raw: str) -> list[str]:
    """解析关键词输入（逗号分隔），去重并保持输入顺序。"""
    normalized_raw = (raw or "").replace("，", ",")
    items = [item.strip() for item in normalized_raw.split(",")]
    items = [item for item in items if item]
    seen = set()
    deduped = []
    for item in items:
        key = item.lower()
        if key not in seen:
            deduped.append(item)
            seen.add(key)
    return deduped


def build_subcategory_label(category: str, category_node: str) -> str:
    """拼接 New Releases 细分类目标识。"""
    c = (category or "").strip()
    n = (category_node or "").strip()
    if c and n:
        return f"{c}:{n}"
    return c or n


def to_int(value: Any, default: int = 0) -> int:
    """容错解析整数：支持 None、数字、字符串(含逗号/符号)。"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else default


def to_nullable_int(value: Any) -> Any:
    """容错解析排名等整数，失败返回 pd.NA。"""
    if value is None or value == "":
        return pd.NA
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else pd.NA


def to_percent(value: Any) -> Any:
    """容错解析百分比字符串（如 +1,250% -> 1250.0）。"""
    if value is None or value == "":
        return pd.NA
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace("%", "").replace(",", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return pd.NA


def to_float(value: Any) -> Any:
    """容错解析浮点数，失败返回 pd.NA。"""
    if value is None or value == "":
        return pd.NA
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return pd.NA


def to_star_rating(value: Any) -> Any:
    """容错解析星级评分，支持 '4.6 out of 5 stars' 等格式。"""
    if value is None or value == "":
        return pd.NA
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return pd.NA
    try:
        return float(match.group(1))
    except ValueError:
        return pd.NA


def _normalize_scalar(value: Any) -> Any:
    """将常见嵌套字段值压平为标量。"""
    if isinstance(value, dict):
        for key in ("value", "raw", "text", "display", "amount", "current_price"):
            if key in value and value[key] not in (None, ""):
                return _normalize_scalar(value[key])
        # 兜底：递归取第一个可用标量
        for v in value.values():
            cand = _normalize_scalar(v)
            if is_present_scalar(cand):
                return cand
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        for v in value:
            cand = _normalize_scalar(v)
            if is_present_scalar(cand):
                return cand
        return ""
    return value


def get_first(item: dict[str, Any], *paths: str, default: Any = "") -> Any:
    """按候选路径取第一个非空值；支持 a.b.c 形式路径。"""
    for path in paths:
        current: Any = item
        ok = True
        for part in path.split("."):
            if not isinstance(current, dict):
                ok = False
                break
            if part in current:
                current = current[part]
                continue

            # 兼容不同命名风格：如 productTitle / Product_Title / product-title
            lower_part = part.lower()
            normalized_part = normalize_key_name(part)
            matched = MISSING
            for k, v in current.items():
                key_str = str(k)
                if key_str.lower() == lower_part or normalize_key_name(key_str) == normalized_part:
                    matched = v
                    break
            if matched is MISSING:
                ok = False
                break
            current = matched
        if not ok:
            continue
        current = _normalize_scalar(current)
        if current not in (None, ""):
            return current
    return default


def normalize_key_name(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def is_present_scalar(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    return s != "" and s.lower() not in {"none", "null", "nan"}


def find_value_by_key_hints(
    data: Any,
    include_hints: tuple[str, ...],
    exclude_hints: tuple[str, ...] = (),
) -> Any:
    """
    在嵌套结构中按字段名关键词查找值（BFS）。
    例如 include_hints=('title','name') 可用于兜底找标题字段。
    """
    include = tuple(normalize_key_name(h) for h in include_hints)
    exclude = tuple(normalize_key_name(h) for h in exclude_hints)
    queue: list[Any] = [data]

    while queue:
        node = queue.pop(0)
        if isinstance(node, dict):
            for k, v in node.items():
                nk = normalize_key_name(k)
                if any(h in nk for h in include) and not any(h in nk for h in exclude):
                    cand = _normalize_scalar(v)
                    if is_present_scalar(cand):
                        return cand
                if isinstance(v, (dict, list)):
                    queue.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    queue.append(v)
    return ""


def derive_title_from_url(url: str, asin: str = "") -> str:
    """从商品 URL 尝试推断标题（slug -> words）。"""
    clean = (url or "").strip()
    if not clean:
        return ""
    parsed = urlparse(clean)
    parts = [p for p in parsed.path.split("/") if p]
    if "dp" in parts:
        idx = parts.index("dp")
        if idx > 0:
            slug = parts[idx - 1]
            slug = re.sub(r"[-_]+", " ", slug).strip()
            if slug and slug.lower() != (asin or "").lower():
                return slug
    return ""


def parse_iso_date(value: str) -> Optional[datetime.date]:
    try:
        return datetime.fromisoformat(value).date()
    except (TypeError, ValueError):
        return None


def highlight_title(title: str, keywords: list[str]) -> str:
    """将标题中命中的关键词高亮。"""
    if not title:
        return ""
    if not keywords:
        return html.escape(title)

    ordered_keywords = sorted({kw for kw in keywords if kw}, key=len, reverse=True)
    if not ordered_keywords:
        return html.escape(title)

    pattern = re.compile("|".join(re.escape(kw) for kw in ordered_keywords), re.IGNORECASE)
    result = []
    last_end = 0

    for match in pattern.finditer(title):
        result.append(html.escape(title[last_end : match.start()]))
        result.append(
            "<mark style='background-color:#ffd8d8;color:#b00020;font-weight:700;padding:0 2px;border-radius:3px;'>"
            f"{html.escape(match.group(0))}"
            "</mark>"
        )
        last_end = match.end()

    result.append(html.escape(title[last_end:]))
    return "".join(result)


def hit_count(title: str, keywords: list[str]) -> int:
    """统计标题命中的关键词数量（按关键词去重计数）。"""
    lowered = (title or "").lower()
    return sum(1 for kw in keywords if kw.lower() in lowered)


def as_html_link(url: str, text: str = "Open") -> str:
    """将 URL 转成 HTML 链接。"""
    clean = (url or "").strip()
    if not clean:
        return ""
    safe_url = html.escape(clean, quote=True)
    safe_text = html.escape(text)
    return f"<a href='{safe_url}' target='_blank'>{safe_text}</a>"


def normalize_image_url(url: str) -> str:
    """标准化图片 URL，兼容 // 开头、相对路径和无协议域名。"""
    clean = (url or "").strip()
    if not clean:
        return ""
    if clean.startswith("//"):
        return f"https:{clean}"
    if clean.startswith("/"):
        return f"https://m.media-amazon.com{clean}"
    if re.match(r"^https?://", clean, flags=re.IGNORECASE):
        return clean
    if clean.startswith("www."):
        return f"https://{clean}"
    if "m.media-amazon.com" in clean and not clean.lower().startswith("http"):
        return f"https://{clean.lstrip('/')}"
    return clean


def extract_photo_url(item: dict[str, Any]) -> str:
    """容错提取商品图片 URL。"""
    direct = str(
        get_first(
            item,
            "product_photo",
            "product_image",
            "photo",
            "image",
            "image_url",
            "thumbnail",
            "main_image",
            "main_image_url",
            "product.image",
            default="",
        )
    ).strip()
    direct = normalize_image_url(direct)
    if direct and (direct.lower().startswith("http") or direct.startswith("data:image/")):
        return direct

    hint = str(
        find_value_by_key_hints(
            item,
            include_hints=("image", "photo", "thumbnail", "picture", "img"),
            exclude_hints=("width", "height", "rank", "review", "rating", "price"),
        )
    ).strip()
    hint = normalize_image_url(hint)
    if hint and (hint.lower().startswith("http") or hint.startswith("data:image/")):
        return hint
    return ""


def as_html_image(url: str, width: int = 72) -> str:
    """将图片 URL 转成 HTML 图片标签。"""
    clean = normalize_image_url(url)
    if not clean:
        return ""
    safe_url = html.escape(clean, quote=True)
    return (
        f"<img src='{safe_url}' alt='product' "
        f"style='width:{width}px;height:{width}px;object-fit:contain;border-radius:6px;'/>"
    )


def build_headers(api_key: str) -> dict[str, str]:
    return {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": API_HOST,
    }


def sanitize_api_key(raw: str) -> str:
    """清洗 API Key，移除常见复制导致的引号和空白字符。"""
    key = (raw or "").strip()
    key = key.strip(" \t\r\n'\"`“”‘’")
    # 去掉零宽字符，避免 header 编码异常
    key = key.replace("\u200b", "").replace("\ufeff", "")
    return key


def is_latin1_encodable(value: str) -> bool:
    try:
        value.encode("latin-1")
        return True
    except UnicodeEncodeError:
        return False


def normalize_marketplace(raw: str) -> str:
    """规范化 marketplace，兼容常见缩写。"""
    value = (raw or "").strip().lower()
    alias = {
        "uk": "co.uk",
        "gb": "co.uk",
        "us": "com",
        "usa": "com",
    }
    return alias.get(value, value or "co.uk")


def marketplace_from_amazon_host(host: str) -> str:
    host = host.lower()
    if host.endswith("amazon.co.uk"):
        return "co.uk"
    if host.endswith("amazon.com"):
        return "com"
    match = re.search(r"amazon\.(.+)$", host)
    return normalize_marketplace(match.group(1)) if match else "co.uk"


def parse_task_from_rank_url(url: str, endpoint: str) -> Optional[CollectionTask]:
    """从 Amazon 榜单 URL 提取 endpoint 采集参数。"""
    parsed = urlparse((url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return None

    path_parts = [part for part in parsed.path.split("/") if part]
    endpoint_tokens = {"new-releases", "newreleases"} if endpoint == "new-releases" else {"movers-and-shakers"}
    idx = next((i for i, part in enumerate(path_parts) if part.lower() in endpoint_tokens), -1)
    if idx < 0:
        return None

    category = ""
    category_node = ""
    if idx + 1 < len(path_parts):
        second = path_parts[idx + 1]
        if second.isdigit():
            category_node = second
        else:
            category = second

    if endpoint == "new-releases" and idx + 2 < len(path_parts):
        third = path_parts[idx + 2]
        if third.isdigit():
            category_node = third

    query = parse_qs(parsed.query)
    page = 1
    for key in ("pg", "page"):
        if key in query and query[key]:
            page = max(1, to_int(query[key][0], default=1))
            break

    if not category and not category_node:
        return None

    return CollectionTask(
        endpoint=endpoint,
        marketplace=marketplace_from_amazon_host(parsed.netloc),
        category=category,
        category_node=category_node,
        page=page,
        source_url=url,
    )


def build_fixed_tasks(urls: list[str], endpoint: str) -> list[CollectionTask]:
    """将固定 URL 列表转成去重后的采集任务。"""
    tasks: list[CollectionTask] = []
    seen: set[tuple[str, str, str, int]] = set()

    for raw in urls:
        task = parse_task_from_rank_url(raw, endpoint)
        if not task:
            continue
        key = (task.marketplace, task.category, task.category_node, task.page)
        if key in seen:
            continue
        seen.add(key)
        tasks.append(task)
    return tasks


def fetch_products(
    endpoint: str,
    api_key: str,
    marketplace: str,
    category: str,
    page: int,
    language: str,
    category_node: str = "",
) -> tuple[list[dict[str, Any]], bool]:
    """最小请求封装：返回 (data.products, has_error)。"""
    url = f"{API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    normalized_marketplace = normalize_marketplace(marketplace)
    params: dict[str, Any] = {"marketplace": normalized_marketplace, "page": int(page)}

    if category.strip():
        params["category"] = category.strip()
    if endpoint == "new-releases" and category_node:
        params["category_node"] = category_node
    if language.strip():
        params["language"] = language.strip()

    try:
        response = requests.get(url, headers=build_headers(api_key), params=params, timeout=20)
    except UnicodeEncodeError as exc:
        st.error(
            f"[{endpoint}] 请求头编码失败：{exc}。请检查 `{API_KEY_ENV}` 是否包含中文引号或特殊字符。"
        )
        return [], True
    except requests.RequestException as exc:
        st.error(f"[{endpoint}] 请求失败: {exc}")
        return [], True

    try:
        payload = response.json()
    except ValueError:
        payload = None

    if response.status_code >= 400:
        api_message = None
        if isinstance(payload, dict):
            api_message = payload.get("message") or payload.get("error")
        message = f"[{endpoint}] 请求失败: HTTP {response.status_code}。"
        if api_message:
            message += f" {api_message}"
        if response.status_code == 422:
            message += " 建议检查 marketplace（英区请用 co.uk）、category（可先留空或使用 kids）、language（可先留空）。"
        st.error(message)
        st.caption(f"请求参数: {params}")
        return [], True

    if payload is None:
        st.error(f"[{endpoint}] 返回不是合法 JSON。")
        return [], True

    if not isinstance(payload, dict):
        st.error(f"[{endpoint}] 返回结构异常（非对象）。")
        return [], True

    status = str(payload.get("status", "")).strip().lower()
    raw_status = payload.get("status")
    if isinstance(raw_status, bool):
        status_ok = raw_status
    else:
        status_ok = status in ("ok", "true", "success")
    if raw_status not in (None, "") and not status_ok:
        st.warning(f"[{endpoint}] API 状态非 OK：{raw_status}")

    data = payload.get("data", {})
    products: Any
    if isinstance(data, dict):
        products = data.get("products")
        if products is None:
            products = data.get("items")
        if products is None:
            products = data.get("results")
        if products is None and isinstance(data.get("list"), list):
            products = data.get("list")
        if products is None:
            products = []
    elif isinstance(data, list):
        products = data
    else:
        products = []

    if not isinstance(products, list):
        st.error(f"[{endpoint}] data.products 结构异常。")
        return [], True
    return products, False


def _is_empty_raw_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def dedupe_products(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按 ASIN 去重；同 ASIN 合并字段（优先保留非空值），避免标题等信息丢失。"""
    by_key: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    for item in products:
        if not isinstance(item, dict):
            continue
        asin = str(get_first(item, "asin", "ASIN", "product_asin", "product.id", default="")).strip()
        fallback = str(get_first(item, "product_url", "url", "product_title", "title", default="")).strip()
        key = asin or fallback
        if not key:
            continue

        if key not in by_key:
            by_key[key] = dict(item)
            order.append(key)
            continue

        merged = by_key[key]
        for k, v in item.items():
            if k not in merged or _is_empty_raw_value(merged.get(k)):
                merged[k] = v
        by_key[key] = merged

    return [by_key[k] for k in order]


def collect_products_for_tasks(
    api_key: str,
    tasks: list[CollectionTask],
    language: str,
) -> tuple[list[dict[str, Any]], int]:
    """按任务批量采集，返回合并去重结果与失败次数。"""
    merged: list[dict[str, Any]] = []
    errors = 0
    for task in tasks:
        products, has_error = fetch_products(
            endpoint=task.endpoint,
            api_key=api_key,
            marketplace=task.marketplace,
            category=task.category,
            category_node=task.category_node,
            page=task.page,
            language=language,
        )
        if has_error:
            errors += 1
        for item in products:
            if not isinstance(item, dict):
                continue
            enriched_item = dict(item)
            enriched_item.setdefault("__task_category", task.category)
            enriched_item.setdefault("__task_category_node", task.category_node)
            enriched_item.setdefault("__task_source_url", task.source_url)
            merged.append(enriched_item)
    return dedupe_products(merged), errors


def fetch_new_releases(
    api_key: str,
    marketplace: str,
    category: str,
    page: int,
    language: str,
    category_node: str = "",
) -> tuple[list[dict[str, Any]], bool]:
    return fetch_products(
        "new-releases",
        api_key,
        marketplace,
        category,
        page,
        language,
        category_node=category_node,
    )


def fetch_movers_and_shakers(
    api_key: str,
    marketplace: str,
    category: str,
    page: int,
    language: str,
) -> tuple[list[dict[str, Any]], bool]:
    return fetch_products("movers-and-shakers", api_key, marketplace, category, page, language)


def load_new_release_history() -> dict[str, Any]:
    if not NEW_RELEASE_HISTORY_FILE.exists():
        return {"by_asin": {}}
    try:
        data = json.loads(NEW_RELEASE_HISTORY_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("by_asin"), dict):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return {"by_asin": {}}


def save_new_release_history(history: dict[str, Any]) -> None:
    NEW_RELEASE_HISTORY_FILE.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def enrich_new_release_history_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """基于本地快照计算 Days_On_List 与 Rank_Change_Percent。"""
    if df.empty:
        return df

    history = load_new_release_history()
    by_asin: dict[str, Any] = history.setdefault("by_asin", {})

    today = datetime.now().date()
    today_s = today.isoformat()
    retention_cutoff = today - timedelta(days=HISTORY_RETENTION_DAYS)
    lookback_start = today - timedelta(days=RANK_CHANGE_LOOKBACK_DAYS)

    enriched = df.copy()
    computed_days: list[Any] = []
    computed_change: list[Any] = []

    for _, row in enriched.iterrows():
        asin = str(row.get("ASIN", "") or "").strip()
        rank_val = pd.to_numeric(row.get("Rank"), errors="coerce")
        rank_int = int(rank_val) if pd.notna(rank_val) else None

        if not asin:
            computed_days.append(pd.NA)
            computed_change.append(pd.NA)
            continue

        entry = by_asin.get(asin, {})
        first_seen = parse_iso_date(entry.get("first_seen", "")) if isinstance(entry, dict) else None
        if first_seen is None:
            first_seen = today

        raw_ranks = entry.get("ranks", []) if isinstance(entry, dict) else []
        ranks: list[dict[str, Any]] = []
        for point in raw_ranks:
            if not isinstance(point, dict):
                continue
            d = parse_iso_date(point.get("date", ""))
            r = to_int(point.get("rank"), default=0)
            if d is None or r <= 0:
                continue
            if d >= retention_cutoff:
                ranks.append({"date": d.isoformat(), "rank": r})

        # upsert today's rank
        if rank_int is not None and rank_int > 0:
            found_today = False
            for point in ranks:
                if point["date"] == today_s:
                    point["rank"] = rank_int
                    found_today = True
                    break
            if not found_today:
                ranks.append({"date": today_s, "rank": rank_int})

        ranks.sort(key=lambda x: x["date"])

        # 计算 days_on_list
        days_on_list = (today - first_seen).days + 1
        computed_days.append(max(days_on_list, 1))

        # 计算 rank_change_percent（近3天最早可用 rank 对比当前 rank）
        change_percent = pd.NA
        if rank_int is not None and rank_int > 0:
            candidates = []
            for point in ranks:
                d = parse_iso_date(point["date"])
                if d is None:
                    continue
                if lookback_start <= d < today and to_int(point.get("rank"), 0) > 0:
                    candidates.append(point)
            if candidates:
                baseline = to_int(candidates[0]["rank"], 0)
                if baseline > 0:
                    change_percent = (baseline - rank_int) / baseline * 100
        computed_change.append(change_percent)

        by_asin[asin] = {
            "first_seen": first_seen.isoformat(),
            "last_seen": today_s,
            "ranks": ranks,
        }

    enriched["Days_On_List"] = pd.to_numeric(computed_days, errors="coerce")
    # 如果 API 本身有该字段就保留，否则用本地快照计算值
    if "Rank_Change_Percent" in enriched.columns:
        existing_change = pd.to_numeric(enriched["Rank_Change_Percent"], errors="coerce")
    else:
        existing_change = pd.Series(float("nan"), index=enriched.index, dtype="float")
    fallback_change = pd.Series(
        pd.to_numeric(computed_change, errors="coerce"),
        index=enriched.index,
        dtype="float",
    )
    enriched["Rank_Change_Percent"] = existing_change.fillna(fallback_change)

    save_new_release_history(history)
    return enriched


def map_new_releases(products: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in products:
        asin = str(get_first(item, "asin", "ASIN", "product_asin", "product.id", default="")).strip()

        title = str(
            get_first(
                item,
                "product_title",
                "title",
                "product_name",
                "name",
                "product.title",
                default="",
            )
        ).strip()
        if not title:
            title = str(
                find_value_by_key_hints(
                    item,
                    include_hints=("title", "name", "productname", "itemname", "description"),
                    exclude_hints=("url", "link", "image", "photo", "asin", "rank", "price"),
                )
            ).strip()

        rank = to_nullable_int(
            get_first(
                item,
                "rank",
                "sales_rank",
                "current_rank",
                "position",
                "index",
                "product.rank",
                default=None,
            )
        )
        if pd.isna(rank):
            rank = to_nullable_int(
                find_value_by_key_hints(
                    item,
                    include_hints=("rank", "position", "index"),
                    exclude_hints=("change",),
                )
            )

        reviews = to_int(
            get_first(
                item,
                "product_num_ratings",
                "num_ratings",
                "rating_count",
                "reviews",
                "reviews_count",
                "product.reviews_count",
                default=0,
            ),
            default=0,
        )
        if reviews == 0:
            reviews = to_int(
                find_value_by_key_hints(
                    item,
                    include_hints=("reviews", "ratings", "ratingcount", "reviewcount"),
                ),
                default=0,
            )

        category = str(
            get_first(
                item,
                "category",
                "product_category",
                "department",
                "browse_node_name",
                default=item.get("__task_category", ""),
            )
        ).strip()
        category_node = str(
            get_first(
                item,
                "category_node",
                "browse_node",
                "browse_node_id",
                "node",
                default=item.get("__task_category_node", ""),
            )
        ).strip()
        subcategory = build_subcategory_label(category, category_node)

        price = str(
            get_first(
                item,
                "product_price",
                "price",
                "display_price",
                "current_price",
                "product.price",
                default="",
            )
        ).strip()
        if not price:
            price = str(
                find_value_by_key_hints(
                    item,
                    include_hints=("price", "amount", "cost"),
                    exclude_hints=("shipping", "discount"),
                )
            ).strip()

        product_url = str(
            get_first(
                item,
                "product_url",
                "url",
                "link",
                "product_link",
                "product.url",
                default="",
            )
        ).strip()

        photo = extract_photo_url(item)

        rows.append(
            {
                "ASIN": asin,
                "Title": title,
                "Rank": rank,
                "Reviews": reviews,
                "Category": category,
                "Category_Node": category_node,
                "Subcategory": subcategory,
                "star_rating": to_star_rating(
                    get_first(
                        item,
                        "product_star_rating",
                        "star_rating",
                        "rating",
                        "product.rating",
                        default=find_value_by_key_hints(
                            item,
                            include_hints=("star", "rating"),
                            exclude_hints=("num", "count", "review"),
                        ),
                    )
                ),
                "Price": price,
                "Product_URL": product_url,
                "Product_Photo": photo,
                # 阶段2先保留接口位，后续快照累计后再完整计算
                "Days_On_List": to_nullable_int(
                    get_first(item, "days_on_list", "days_on_chart", "new_release_days", default=None)
                ),
                "Rank_Change_Percent": to_percent(
                    get_first(
                        item,
                        "rank_change_percent",
                        "sales_rank_change",
                        "rank_change",
                        default=None,
                    )
                ),
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "ASIN",
            "Title",
            "Rank",
            "Reviews",
            "Category",
            "Category_Node",
            "Subcategory",
            "star_rating",
            "Price",
            "Product_URL",
            "Product_Photo",
            "Days_On_List",
            "Rank_Change_Percent",
        ],
    )
    if not df.empty:
        df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce").astype("Int64")
        df["Reviews"] = pd.to_numeric(df["Reviews"], errors="coerce").fillna(0).astype(int)
        df["star_rating"] = pd.to_numeric(df["star_rating"], errors="coerce")
        df["Days_On_List"] = pd.to_numeric(df["Days_On_List"], errors="coerce")
        df["Rank_Change_Percent"] = pd.to_numeric(df["Rank_Change_Percent"], errors="coerce")
        fallback_title = df.apply(
            lambda r: derive_title_from_url(str(r.get("Product_URL", "")), str(r.get("ASIN", ""))),
            axis=1,
        )
        df["Title"] = df["Title"].replace("", pd.NA).fillna(fallback_title).fillna(df["ASIN"])
        missing_url = df["Product_URL"].astype(str).str.strip().eq("")
        df.loc[missing_url, "Product_URL"] = df.loc[missing_url, "ASIN"].map(
            lambda x: f"https://www.amazon.co.uk/dp/{x}" if str(x).strip() else ""
        )
    return df


def map_movers_and_shakers(products: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in products:
        asin = str(get_first(item, "asin", "ASIN", "product_asin", "product.id", default="")).strip()

        title = str(
            get_first(
                item,
                "product_title",
                "title",
                "product_name",
                "name",
                "product.title",
                default="",
            )
        ).strip()
        if not title:
            title = str(
                find_value_by_key_hints(
                    item,
                    include_hints=("title", "name", "productname", "itemname", "description"),
                    exclude_hints=("url", "link", "image", "photo", "asin", "rank", "price"),
                )
            ).strip()

        rank = to_nullable_int(
            get_first(
                item,
                "rank",
                "sales_rank",
                "current_rank",
                "position",
                "index",
                "product.rank",
                default=None,
            )
        )
        if pd.isna(rank):
            rank = to_nullable_int(
                find_value_by_key_hints(
                    item,
                    include_hints=("rank", "position", "index"),
                    exclude_hints=("change",),
                )
            )

        product_num_ratings = to_int(
            get_first(
                item,
                "product_num_ratings",
                "num_ratings",
                "rating_count",
                "reviews",
                "reviews_count",
                "product.reviews_count",
                default=0,
            ),
            default=0,
        )
        if product_num_ratings == 0:
            product_num_ratings = to_int(
                find_value_by_key_hints(
                    item,
                    include_hints=("reviews", "ratings", "ratingcount", "reviewcount"),
                ),
                default=0,
            )

        category = str(
            get_first(
                item,
                "category",
                "product_category",
                "department",
                "browse_node_name",
                default=item.get("__task_category", ""),
            )
        ).strip()

        price = str(
            get_first(
                item,
                "product_price",
                "price",
                "display_price",
                "current_price",
                "product.price",
                default="",
            )
        ).strip()
        if not price:
            price = str(
                find_value_by_key_hints(
                    item,
                    include_hints=("price", "amount", "cost"),
                    exclude_hints=("shipping", "discount"),
                )
            ).strip()

        product_url = str(
            get_first(
                item,
                "product_url",
                "url",
                "link",
                "product_link",
                "product.url",
                default="",
            )
        ).strip()
        photo = extract_photo_url(item)

        sales_rank_change = str(
            get_first(
                item,
                "sales_rank_change",
                "rank_change",
                "rank_change_percent",
                default="",
            )
            or ""
        ).strip()
        if not sales_rank_change:
            sales_rank_change = str(
                find_value_by_key_hints(
                    item,
                    include_hints=("rankchange", "salesrankchange", "changepercent"),
                )
                or ""
            ).strip()

        current_rank = to_nullable_int(
            get_first(item, "current_rank", "rank_now", "currentrank", default=None)
        )
        previous_rank = to_nullable_int(
            get_first(item, "previous_rank", "rank_before", "previousrank", default=None)
        )

        rows.append(
            {
                "ASIN": asin,
                "Title": title,
                "Rank": rank,
                "Product_Num_Ratings": product_num_ratings,
                "Category": category,
                "star_rating": to_star_rating(
                    get_first(
                        item,
                        "product_star_rating",
                        "star_rating",
                        "rating",
                        "product.rating",
                        default=find_value_by_key_hints(
                            item,
                            include_hints=("star", "rating"),
                            exclude_hints=("num", "count", "review"),
                        ),
                    )
                ),
                "Price": price,
                "Product_URL": product_url,
                "Product_Photo": photo,
                "Sales_Rank_Change": sales_rank_change,
                "Current_Rank": current_rank,
                "Previous_Rank": previous_rank,
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "ASIN",
            "Title",
            "Rank",
            "Product_Num_Ratings",
            "Category",
            "star_rating",
            "Price",
            "Product_URL",
            "Product_Photo",
            "Sales_Rank_Change",
            "Current_Rank",
            "Previous_Rank",
        ],
    )
    if not df.empty:
        df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce").astype("Int64")
        df["Product_Num_Ratings"] = (
            pd.to_numeric(df["Product_Num_Ratings"], errors="coerce").fillna(0).astype(int)
        )
        df["star_rating"] = pd.to_numeric(df["star_rating"], errors="coerce")
        df["Current_Rank"] = pd.to_numeric(df["Current_Rank"], errors="coerce").astype("Int64")
        df["Previous_Rank"] = pd.to_numeric(df["Previous_Rank"], errors="coerce").astype("Int64")
        fallback_title = df.apply(
            lambda r: derive_title_from_url(str(r.get("Product_URL", "")), str(r.get("ASIN", ""))),
            axis=1,
        )
        df["Title"] = df["Title"].replace("", pd.NA).fillna(fallback_title).fillna(df["ASIN"])
        missing_url = df["Product_URL"].astype(str).str.strip().eq("")
        df.loc[missing_url, "Product_URL"] = df.loc[missing_url, "ASIN"].map(
            lambda x: f"https://www.amazon.co.uk/dp/{x}" if str(x).strip() else ""
        )
        # Sales_Rank_Change 兜底计算
        missing_change = df["Sales_Rank_Change"].astype(str).str.strip().eq("")
        valid_prev = pd.to_numeric(df["Previous_Rank"], errors="coerce")
        valid_cur = pd.to_numeric(df["Current_Rank"], errors="coerce")
        calc_change = (valid_prev - valid_cur) / valid_prev * 100
        df.loc[missing_change & valid_prev.gt(0) & valid_cur.gt(0), "Sales_Rank_Change"] = (
            calc_change.map(lambda v: f"{v:+.1f}%")
        )
        # Movers 的潜力筛选统一以 product_num_ratings 作为评论数
        df["Reviews"] = df["Product_Num_Ratings"]
    return df


def prepare_table(df: pd.DataFrame, keywords: list[str], image_size_px: int) -> pd.DataFrame:
    table = df.copy()
    title_series = (
        table["Title"]
        if "Title" in table.columns
        else table["ASIN"] if "ASIN" in table.columns else pd.Series("", index=table.index)
    )
    asin_series = table["ASIN"] if "ASIN" in table.columns else pd.Series("", index=table.index)
    safe_title = title_series.fillna("").astype(str).str.strip()
    safe_title = safe_title.where(safe_title.ne(""), asin_series.fillna("").astype(str))
    safe_title = safe_title.fillna("").astype(str)

    table["Title"] = safe_title
    table["Hit Count"] = table["Title"].map(lambda x: hit_count(str(x), keywords))
    table["Highlighted Title"] = table["Title"].map(lambda x: highlight_title(str(x), keywords))
    # 兜底：高亮列为空时回退普通标题，避免 Title 空白
    table["Highlighted Title"] = table["Highlighted Title"].replace("", pd.NA).fillna(
        table["Title"].map(lambda x: html.escape(str(x)))
    )

    photo_series = table["Product_Photo"] if "Product_Photo" in table.columns else pd.Series("", index=table.index)
    url_series = table["Product_URL"] if "Product_URL" in table.columns else pd.Series("", index=table.index)
    table["Image"] = photo_series.fillna("").map(lambda x: as_html_image(str(x), width=image_size_px))
    table["product_photo_raw"] = photo_series.fillna("").map(lambda x: normalize_image_url(str(x)))
    table["product_url_raw"] = url_series.fillna("").map(lambda x: str(x))
    table["product_url"] = table["product_url_raw"].map(lambda x: as_html_link(str(x), text="View"))
    if "Product_Num_Ratings" in table.columns:
        table["product_num_ratings"] = table["Product_Num_Ratings"]
    return table


def load_favorites() -> list[dict[str, Any]]:
    if not FAVORITES_FILE.exists():
        return []
    try:
        content = json.loads(FAVORITES_FILE.read_text(encoding="utf-8"))
        return content if isinstance(content, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_favorites(rows: list[dict[str, Any]]) -> None:
    FAVORITES_FILE.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def append_favorites(selected_df: pd.DataFrame, board: str) -> tuple[int, int]:
    saved = load_favorites()
    existing_keys = set()
    for item in saved:
        key = (str(item.get("board", "")), str(item.get("asin", "")) or str(item.get("title", "")))
        existing_keys.add(key)

    added = 0
    now_ts = datetime.now().isoformat(timespec="seconds")

    for row in selected_df.to_dict(orient="records"):
        asin = str(row.get("ASIN", "") or "").strip()
        title = str(row.get("Title", "") or "").strip()
        unique = asin or title
        if not unique:
            continue

        key = (board, unique)
        if key in existing_keys:
            continue

        saved.append(
            {
                "board": board,
                "asin": asin,
                "title": title,
                "rank": row.get("Rank"),
                "reviews": row.get("Reviews"),
                "price": row.get("Price"),
                "hit_count": row.get("Hit Count"),
                "saved_at": now_ts,
            }
        )
        existing_keys.add(key)
        added += 1

    save_favorites(saved)
    return added, len(saved)


def render_html_table(
    df: pd.DataFrame,
    columns: list[str],
    table_key: str,
    table_width_px: int,
    row_height_px: int,
    title_column: str = "Highlighted Title",
    column_widths: Optional[dict[str, int]] = None,
) -> None:
    if df.empty:
        st.info("当前没有可展示的数据。")
        return

    table = df[columns].copy()
    if title_column in table.columns:
        table = table.rename(columns={title_column: "Title"})
    if "Days_On_List" in table.columns:
        series = pd.to_numeric(table["Days_On_List"], errors="coerce")
        table["Days_On_List"] = series.map(lambda x: f"{int(x)}" if pd.notna(x) else "")
    if "Rank_Change_Percent" in table.columns:
        series = pd.to_numeric(table["Rank_Change_Percent"], errors="coerce")
        table["Rank_Change_Percent"] = series.map(lambda x: f"{x:.1f}%" if pd.notna(x) else "")
    if "Rank" in table.columns:
        series = pd.to_numeric(table["Rank"], errors="coerce")
        table["Rank"] = series.map(lambda x: f"{int(x)}" if pd.notna(x) else "")
    if "star_rating" in table.columns:
        series = pd.to_numeric(table["star_rating"], errors="coerce")
        table["star_rating"] = series.map(lambda x: f"{x:.1f}" if pd.notna(x) else "")
    # 避免 Int64 列与空字符串混合时触发 TypeError
    table = table.astype("object")
    table = table.where(pd.notna(table), None)
    table = table.applymap(lambda v: "" if v is None else v)
    html_table = table.to_html(index=False, escape=False)
    width_css = ""
    if column_widths:
        for col_name, width in column_widths.items():
            if col_name not in table.columns:
                continue
            idx = table.columns.get_loc(col_name) + 1  # CSS nth-child 从 1 开始
            width_css += (
                f".table-wrap-{table_key} th:nth-child({idx}),"
                f".table-wrap-{table_key} td:nth-child({idx})"
                f"{{width:{int(width)}px;max-width:{int(width)}px;white-space:normal;word-break:break-word;}}"
            )
    st.markdown(
        f"""
<style>
.table-wrap-{table_key} {{
  overflow-x: auto;
  max-width: 100%;
}}
.table-wrap-{table_key} table {{
  width: {table_width_px}px;
  min-width: 100%;
}}
.table-wrap-{table_key} th,
.table-wrap-{table_key} td {{
  height: {row_height_px}px;
  vertical-align: middle;
}}
.table-wrap-{table_key} td img {{
  max-height: {max(row_height_px - 8, 24)}px;
  width: auto;
}}
{width_css}
</style>
<div class="table-wrap-{table_key}">
{html_table}
</div>
""",
        unsafe_allow_html=True,
    )


def build_download_df(df: pd.DataFrame, board: str) -> pd.DataFrame:
    if board == "new_releases":
        columns = [
            "ASIN",
            "Title",
            "Rank",
            "Reviews",
            "Subcategory",
            "Category",
            "Category_Node",
            "star_rating",
            "Price",
            "product_photo_raw",
            "product_url_raw",
            "Days_On_List",
            "Rank_Change_Percent",
            "Hit Count",
        ]
    else:
        columns = [
            "ASIN",
            "Title",
            "Rank",
            "product_num_ratings",
            "Category",
            "star_rating",
            "Price",
            "product_photo_raw",
            "product_url_raw",
            "Sales_Rank_Change",
            "Current_Rank",
            "Previous_Rank",
            "Hit Count",
        ]

    available = [c for c in columns if c in df.columns]
    out = df[available].copy()
    rename_map = {}
    if "product_url_raw" in out.columns:
        rename_map["product_url_raw"] = "product_url"
    if "product_photo_raw" in out.columns:
        rename_map["product_photo_raw"] = "image_url"
    if rename_map:
        out = out.rename(columns=rename_map)
    return out


def render_download_button(df: pd.DataFrame, board: str, label: str) -> None:
    if df.empty:
        return
    export_df = build_download_df(df, board)
    csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=f"下载{label}CSV",
        data=csv_bytes,
        file_name=f"{board}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
        key=f"download_{board}",
    )


def render_favorite_section(df: pd.DataFrame, board: str) -> None:
    st.write("### 收藏操作")
    if df.empty:
        st.info("当前无可收藏商品。")
        return

    editor_df = df[
        [col for col in ["ASIN", "Title", "Rank", "Reviews", "Price", "Hit Count"] if col in df.columns]
    ].copy()
    editor_df.insert(0, "Favorite", False)

    edited = st.data_editor(
        editor_df,
        hide_index=True,
        use_container_width=True,
        key=f"editor_{board}",
        column_config={
            "Favorite": st.column_config.CheckboxColumn("收藏", help="勾选后点击下方按钮保存"),
        },
        disabled=[col for col in editor_df.columns if col != "Favorite"],
    )

    if st.button("保存收藏", key=f"save_{board}", use_container_width=True):
        selected = edited[edited["Favorite"]].copy()
        if selected.empty:
            st.warning("请先勾选要收藏的商品。")
            return
        added, total = append_favorites(selected, board)
        if added == 0:
            st.info(f"没有新增收藏（已自动去重）。当前共 {total} 条。")
        else:
            st.success(f"已新增 {added} 条收藏，当前共 {total} 条。")


def empty_new_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ASIN",
            "Title",
            "Rank",
            "Reviews",
            "Category",
            "Category_Node",
            "Subcategory",
            "star_rating",
            "Price",
            "Product_URL",
            "Product_Photo",
            "Days_On_List",
            "Rank_Change_Percent",
        ]
    )


def empty_movers_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ASIN",
            "Title",
            "Rank",
            "Product_Num_Ratings",
            "Reviews",
            "Category",
            "star_rating",
            "Price",
            "Product_URL",
            "Product_Photo",
            "Sales_Rank_Change",
            "Current_Rank",
            "Previous_Rank",
        ]
    )


def init_state() -> None:
    if "new_df" not in st.session_state:
        st.session_state["new_df"] = empty_new_df()
    if "movers_df" not in st.session_state:
        st.session_state["movers_df"] = empty_movers_df()


def main() -> None:
    st.set_page_config(page_title="Amazon UK Trend Scout", layout="wide")
    st.title("Amazon UK Trend Scout")
    st.caption("亚马逊英区选品辅助工具（阶段2：最小可用 API 接入）")

    init_state()
    raw_api_key = os.getenv(API_KEY_ENV, "")
    api_key = sanitize_api_key(raw_api_key)
    new_tasks = build_fixed_tasks(FIXED_NEW_RELEASE_URLS, endpoint="new-releases")
    movers_tasks = build_fixed_tasks(FIXED_MOVERS_URLS, endpoint="movers-and-shakers")

    with st.sidebar:
        st.header("采集配置")
        keywords_input = st.text_area(
            "关键词库（逗号分隔）",
            value=(
                "cool, novtly, ornaments, tiny, cute, handmade, artificial, mini, wedding, "
                "birthday, art, decor, beauty, 2026, worldcup, football, solar, easy, graduation"
            ),
            height=120,
        )
        language = st.text_input("Language（可选，建议先留空）", value="")
        table_width_px = int(st.slider("表格宽度(px)", min_value=1000, max_value=3200, value=1800, step=100))
        row_height_px = int(st.slider("单元格高度(px)", min_value=36, max_value=140, value=72, step=2))
        start = st.button("开始采集", type="primary", use_container_width=True)
        st.caption(
            f"固定采集任务：New Releases {len(new_tasks)} 个类目任务，"
            f"Movers & Shakers {len(movers_tasks)} 个一级类目任务。"
        )

        if api_key:
            st.success(f"已读取环境变量 `{API_KEY_ENV}`")
            if raw_api_key != api_key:
                st.caption("检测到 API Key 含引号/空白，已自动清洗后使用。")
            if not is_latin1_encodable(api_key):
                st.error(f"`{API_KEY_ENV}` 含非法字符（常见于中文引号），请重新设置后重启。")
        else:
            st.warning(f"未读取到 `{API_KEY_ENV}`，点击开始采集会失败。")

        favorite_total = len(load_favorites())
        st.caption(f"当前本地收藏数: {favorite_total}")

    keywords = parse_keywords(keywords_input)

    if start:
        if not api_key:
            st.error(f"请先设置环境变量 `{API_KEY_ENV}`。")
        elif not is_latin1_encodable(api_key):
            st.error(f"`{API_KEY_ENV}` 含非法字符，无法发送请求。请重新设置后重启。")
        else:
            with st.spinner("正在拉取 Amazon 榜单数据..."):
                new_products, new_errors = collect_products_for_tasks(api_key, new_tasks, language)
                movers_products, movers_errors = collect_products_for_tasks(
                    api_key, movers_tasks, language
                )
                mapped_new = map_new_releases(new_products)
                mapped_movers = map_movers_and_shakers(movers_products)
                st.session_state["new_df"] = enrich_new_release_history_metrics(mapped_new)
                st.session_state["movers_df"] = mapped_movers
            total_errors = new_errors + movers_errors
            if total_errors:
                st.warning(
                    f"采集结束但存在请求错误：失败 {total_errors} 个任务。"
                    f" New Releases {len(st.session_state['new_df'])} 条，"
                    f"Movers & Shakers {len(st.session_state['movers_df'])} 条。"
                )
            else:
                st.success(
                    f"采集完成：New Releases {len(st.session_state['new_df'])} 条，"
                    f"Movers & Shakers {len(st.session_state['movers_df'])} 条。"
                )

    new_tab, movers_tab = st.tabs(["New Releases", "Movers & Shakers"])
    image_size_px = max(row_height_px - 10, 24)

    with new_tab:
        st.subheader("New Releases 看板")
        new_df = prepare_table(st.session_state["new_df"], keywords, image_size_px=image_size_px)
        filtered_new = new_df[
            (pd.to_numeric(new_df["Days_On_List"], errors="coerce") <= 3)
            | (pd.to_numeric(new_df["Rank_Change_Percent"], errors="coerce") > 30)
        ].copy()
        sort_option = st.selectbox(
            "Days_On_List 排序",
            options=[
                "Days_On_List 升序（新上榜优先）",
                "Days_On_List 降序",
                "不按 Days_On_List 排序",
            ],
            index=0,
            key="new_days_sort",
        )
        if sort_option != "不按 Days_On_List 排序" and not filtered_new.empty:
            sort_days = pd.to_numeric(filtered_new["Days_On_List"], errors="coerce")
            filtered_new = filtered_new.assign(_sort_days=sort_days)
            filtered_new = filtered_new.sort_values(
                by=["_sort_days", "Rank"],
                ascending=[sort_option.startswith("Days_On_List 升序"), True],
                na_position="last",
            ).drop(columns=["_sort_days"])
        st.caption("Days_On_List / Rank_Change_Percent 基于本地快照计算。")

        render_html_table(
            filtered_new,
            columns=[
                "Image",
                "ASIN",
                "Subcategory",
                "Highlighted Title",
                "Days_On_List",
                "Rank",
                "Reviews",
                "star_rating",
                "Price",
                "product_url",
                "Rank_Change_Percent",
                "Hit Count",
            ],
            table_key="new_releases",
            table_width_px=table_width_px,
            row_height_px=row_height_px,
            column_widths={
                "Subcategory": 170,
                "Title": 260,
            },
        )
        render_download_button(filtered_new, board="new_releases", label="New Releases")
        render_favorite_section(filtered_new, "new_releases")

    with movers_tab:
        st.subheader("Movers & Shakers 看板")
        movers_df = prepare_table(st.session_state["movers_df"], keywords, image_size_px=image_size_px)
        rating_col = "product_num_ratings" if "product_num_ratings" in movers_df.columns else "Reviews"
        filtered_movers = movers_df[
            pd.to_numeric(movers_df[rating_col], errors="coerce").fillna(0) < 200
        ]

        render_html_table(
            filtered_movers,
            columns=[
                "Image",
                "ASIN",
                "Category",
                "Highlighted Title",
                "product_url",
                "Rank",
                "product_num_ratings",
                "star_rating",
                "Price",
                "Hit Count",
            ],
            table_key="movers_shakers",
            table_width_px=table_width_px,
            row_height_px=row_height_px,
            column_widths={
                "Category": 170,
                "Subcategory": 170,
                "Title": 260,
            },
        )
        render_download_button(filtered_movers, board="movers_and_shakers", label="Movers&Shakers")
        render_favorite_section(filtered_movers, "movers_and_shakers")


if __name__ == "__main__":
    main()
