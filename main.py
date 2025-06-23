import os
import asyncio
import json
import re
import base64
import logging
import random
import string
import math
import socket
from pathlib import Path
from typing import List, Dict, Set, Optional, Any, Tuple, Coroutine
from urllib.parse import urlparse, parse_qs, unquote
import ipaddress
from collections import Counter

# Third-party libraries
import httpx
import aiofiles
import jdatetime
try:
    import geoip2.database
except ImportError:
    print("Error: 'geoip2' library not found. Please install it using: pip install geoip2")
    exit(1)

from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel, Field, model_validator

# ------------------------------------------------------------------------------
# --- FILENAME: core/config.py ---
# ------------------------------------------------------------------------------

class AppConfig:
    """
    Holds all application configuration settings.
    این کلاس تمام تنظیمات برنامه را نگهداری می‌کند.
    """
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    OUTPUT_DIR = BASE_DIR / "sub"

    DIRS = {
        "splitted": OUTPUT_DIR / "splitted",
        "security": OUTPUT_DIR / "security",
        "protocols": OUTPUT_DIR / "protocols",
        "networks": OUTPUT_DIR / "networks",
        "subscribe": OUTPUT_DIR / "subscribe",
        "countries": OUTPUT_DIR / "countries",
    }

    TELEGRAM_CHANNELS_FILE = DATA_DIR / "telegram_channels.json"
    SUBSCRIPTION_LINKS_FILE = DATA_DIR / "subscription_links.json"
    LAST_UPDATE_FILE = DATA_DIR / "last_update.log"
    TELEGRAM_REPORT_FILE = DATA_DIR / "telegram_report.log"
    GEOIP_DB_FILE = DATA_DIR / "GeoLite2-Country.mmdb"

    REMOTE_CHANNELS_URL = "https://raw.githubusercontent.com/PlanAsli/configs-collector-v2ray/main/data/telegram-channel.json"
    GEOIP_DB_URL = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb"

    HTTP_TIMEOUT = 25.0
    HTTP_MAX_REDIRECTS = 5
    HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"}
    MAX_CONCURRENT_REQUESTS = 10
    
    # --- Telegram Scraper Settings ---
    # تنظیمات مربوط به جمع‌آوری از تلگرام
    TELEGRAM_BASE_URL = "https://t.me/s/{}"
    TELEGRAM_SCRAPE_DEPTH = 5
    TELEGRAM_MESSAGE_LIMIT = 30 # NEW: Increased message fetch limit | جدید: افزایش محدودیت دریافت پیام‌ها

    # --- Feature Flags & New Settings ---
    # فلگ‌های ویژگی‌ها و تنظیمات جدید
    ENABLE_SUBSCRIPTION_FETCHING = True
    ENABLE_IP_DEDUPLICATION = True
    ENABLE_LATENCY_TEST = True          # NEW: Enable latency testing | جدید: فعال‌سازی تست سرعت
    LATENCY_TIMEOUT = 5                 # NEW: Timeout for each latency test in seconds | جدید: مهلت زمان تست پینگ
    MAX_LATENCY_TESTS = 150             # NEW: Max configs to test for latency | جدید: حداکثر تعداد کانفیگ برای تست سرعت

    ADD_SIGNATURES = True
    ADV_SIGNATURE = "「 ✨ Free Internet For All 」 @OXNET_IR"
    DNT_SIGNATURE = "❤️ Your Daily Dose of Proxies @OXNET_IR"
    DEV_SIGNATURE = "</> Collector v23.0.0 @OXNET_IR"
    CUSTOM_SIGNATURE = "「 PlanAsli ☕ 」" # NEW: Add your custom signature here | جدید: امضای دلخواه خود را اینجا اضافه کنید


CONFIG = AppConfig()

# ------------------------------------------------------------------------------
# --- FILENAME: core/logger.py ---
# ------------------------------------------------------------------------------

def setup_logger():
    """Configures the root logger for the application."""
    CONFIG.DATA_DIR.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)-8s - %(name)-15s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler()
        ]
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("geoip2").setLevel(logging.WARNING)
    return logging.getLogger("V2RayCollector")

logger = setup_logger()

# ------------------------------------------------------------------------------
# --- FILENAME: core/exceptions.py ---
# ------------------------------------------------------------------------------

class V2RayCollectorException(Exception): pass
class ParsingError(V2RayCollectorException): pass
class NetworkError(V2RayCollectorException): pass

# ------------------------------------------------------------------------------
# --- FILENAME: utils/helpers.py ---
# ------------------------------------------------------------------------------

COUNTRY_CODE_TO_FLAG = {
    'AD': '🇦🇩', 'AE': '🇦🇪', 'AF': '🇦🇫', 'AG': '🇦🇬', 'AI': '🇦🇮', 'AL': '🇦🇱', 'AM': '🇦🇲', 'AO': '🇦🇴', 'AQ': '🇦🇶', 'AR': '🇦🇷', 'AS': '🇦🇸', 'AT': '🇦🇹', 'AU': '🇦🇺', 'AW': '🇦🇼', 'AX': '🇦🇽', 'AZ': '🇦🇿', 'BA': '🇧🇦', 'BB': '🇧🇧',
    'BD': '🇧🇩', 'BE': '🇧🇪', 'BF': '🇧🇫', 'BG': '🇧🇬', 'BH': '🇧🇭', 'BI': '🇧🇮', 'BJ': '🇧🇯', 'BL': '🇧🇱', 'BM': '🇧🇲', 'BN': '🇧🇳', 'BO': '🇧🇴', 'BR': '🇧🇷', 'BS': '🇧🇸', 'BT': '🇧🇹', 'BW': '🇧🇼', 'BY': '🇧🇾', 'BZ': '🇧🇿', 'CA': '🇨�',
    'CC': '🇨🇨', 'CD': '🇨🇩', 'CF': '🇨🇫', 'CG': '🇨🇬', 'CH': '🇨🇭', 'CI': '🇨🇮', 'CK': '🇨🇰', 'CL': '🇨🇱', 'CM': '🇨🇲', 'CN': '🇨🇳', 'CO': '🇨🇴', 'CR': '🇨🇷', 'CU': '🇨🇺', 'CV': '🇨🇻', 'CW': '🇨🇼', 'CX': '🇨🇽', 'CY': '🇨🇾', 'CZ': '🇨🇿',
    'DE': '🇩🇪', 'DJ': '🇩🇯', 'DK': '🇩🇰', 'DM': '🇩🇲', 'DO': '🇩🇴', 'DZ': '🇩🇿', 'EC': '🇪🇨', 'EE': '🇪🇪', 'EG': '🇪🇬', 'ER': '🇪🇷', 'ES': '🇪🇸', 'ET': '🇪🇹', 'FI': '🇫🇮', 'FJ': '🇫🇯', 'FK': '🇫🇰', 'FM': '🇫🇲', 'FO': '🇫🇴', 'FR': '🇫🇷',
    'GA': '🇬🇦', 'GB': '🇬🇧', 'GD': '🇬🇩', 'GE': '🇬🇪', 'GF': '🇬🇫', 'GG': '🇬🇬', 'GH': '🇬🇭', 'GI': '🇬🇮', 'GL': '🇬🇱', 'GM': '🇬🇲', 'GN': '🇬🇳', 'GP': '🇬🇵', 'GQ': '🇬🇶', 'GR': '🇬🇷', 'GS': '🇬🇸', 'GT': '🇬🇹', 'GU': '🇬🇺', 'GW': '🇬🇼',
    'GY': '🇬🇾', 'HK': '🇭🇰', 'HN': '🇭🇳', 'HR': '🇭🇷', 'HT': '🇭🇹', 'HU': '🇭🇺', 'ID': '🇮🇩', 'IE': '🇮🇪', 'IL': '🇮🇱', 'IM': '🇮🇲', 'IN': '🇮🇳', 'IO': '🇮🇴', 'IQ': '🇮🇶', 'IR': '🇮🇷', 'IS': '🇮🇸', 'IT': '🇮🇹', 'JE': '🇯🇪', 'JM': '🇯🇲',
    'JO': '🇯🇴', 'JP': '🇯🇵', 'KE': '🇰🇪', 'KG': '🇰🇬', 'KH': '🇰🇭', 'KI': '🇰🇮', 'KM': '🇰🇲', 'KN': '🇰🇳', 'KP': '🇰🇵', 'KR': '🇰🇷', 'KW': '🇰🇼', 'KY': '🇰🇾', 'KZ': '🇰🇿', 'LA': '🇱🇦', 'LB': '🇱🇧', 'LC': '🇱🇨', 'LI': '🇱🇮', 'LK': '🇱🇰',
    'LR': '🇱🇷', 'LS': '🇱🇸', 'LT': '🇱🇹', 'LU': '🇱🇺', 'LV': '🇱🇻', 'LY': '🇱🇾', 'MA': '🇲🇦', 'MC': '🇲🇨', 'MD': '🇲🇩', 'ME': '🇲🇪', 'MF': '🇲🇫', 'MG': '🇲🇬', 'MH': '🇲🇭', 'MK': '🇲🇰', 'ML': '🇲🇱', 'MM': '🇲🇲', 'MN': '🇲🇳', 'MO': '🇲🇴',
    'MP': '🇲🇵', 'MQ': '🇲🇶', 'MR': '🇲🇷', 'MS': '🇲🇸', 'MT': '🇲🇹', 'MU': '🇲🇺', 'MV': '🇲🇻', 'MW': '🇲🇼', 'MX': '🇲🇽', 'MY': '🇲🇾', 'MZ': '🇲🇿', 'NA': '🇳🇦', 'NC': '🇳🇨', 'NE': '🇳🇪', 'NF': '🇳🇫', 'NG': '🇳🇬', 'NI': '🇳🇮', 'NL': '🇳🇱',
    'NO': '🇳🇴', 'NP': '🇳🇵', 'NR': '🇳🇷', 'NU': '🇳🇺', 'NZ': '🇳🇿', 'OM': '🇴🇲', 'PA': '🇵🇦', 'PE': '🇵🇪', 'PF': '🇵🇫', 'PG': '🇵🇬', 'PH': '🇵🇭', 'PK': '🇵🇰', 'PL': '🇵🇱', 'PM': '🇵🇲', 'PN': '🇵🇳', 'PR': '🇵🇷', 'PS': '🇵🇸', 'PT': '🇵🇹',
    'PW': '🇵🇼', 'PY': '🇵🇾', 'QA': '🇶🇦', 'RE': '🇷🇪', 'RO': '🇷🇴', 'RS': '🇷🇸', 'RU': '🇷🇺', 'RW': '🇷🇼', 'SA': '🇸🇦', 'SB': '🇸🇧', 'SC': '🇸🇨', 'SD': '🇸🇩', 'SE': '🇸🇪', 'SG': '🇸🇬', 'SH': '🇸🇭', 'SI': '🇸🇮', 'SJ': '🇸🇯', 'SK': '🇸🇰',
    'SL': '🇸🇱', 'SM': '🇸🇲', 'SN': '🇸🇳', 'SO': '🇸🇴', 'SR': '🇸🇷', 'SS': '🇸🇸', 'ST': '🇸🇹', 'SV': '🇸🇻', 'SX': '🇸🇽', 'SY': '🇸🇾', 'SZ': '🇸🇿', 'TC': '🇹🇨', 'TD': '🇹🇩', 'TF': '🇹🇫', 'TG': '🇹🇬', 'TH': '🇹🇭', 'TJ': '🇹🇯', 'TK': '🇹🇰',
    'TL': '🇹🇱', 'TM': '🇹🇲', 'TN': '🇹🇳', 'TO': '🇹🇴', 'TR': '🇹🇷', 'TT': '🇹🇹', 'TV': '🇹🇻', 'TW': '🇹🇼', 'TZ': '🇹🇿', 'UA': '🇺🇦', 'UG': '🇺🇬', 'US': '🇺🇸', 'UY': '🇺🇾', 'UZ': '🇺🇿', 'VA': '🇻🇦', 'VC': '🇻🇨', 'VE': '🇻🇪', 'VG': '🇻🇬',
    'VI': '🇻🇮', 'VN': '🇻🇳', 'VU': '🇻🇺', 'WF': '🇼🇫', 'WS': '🇼🇸', 'YE': '🇾🇪', 'YT': '🇾🇹', 'ZA': '🇿🇦', 'ZM': '🇿🇲', 'ZW': '🇿🇼', 'XX': '🏳️'
}

def is_valid_base64(s: str) -> bool:
    try:
        s_padded = s + '=' * (-len(s) % 4)
        return base64.b64encode(base64.b64decode(s_padded)).decode('utf-8') == s_padded
    except (ValueError, TypeError):
        return False

def get_iran_timezone():
    return timezone(timedelta(hours=3, minutes=30))

def generate_random_uuid_string() -> str:
    return '-'.join([''.join(random.choices(string.ascii_lowercase + string.digits, k=k)) for k in [8, 4, 4, 4, 12]])

def is_ip_address(address: str) -> bool:
    try:
        ipaddress.ip_address(address)
        return True
    except ValueError:
        return False

# ------------------------------------------------------------------------------
# --- FILENAME: models/v2ray.py ---
# ------------------------------------------------------------------------------

class BaseConfig(BaseModel):
    model_config = {'str_strip_whitespace': True}
    protocol: str
    host: str
    port: int
    uuid: str
    remarks: str = "N/A"
    network: str = 'tcp'
    security: str = 'none'
    path: Optional[str] = None
    sni: Optional[str] = None
    fingerprint: Optional[str] = None
    country: Optional[str] = Field("XX", exclude=True)
    source_type: str = Field("unknown", exclude=True)
    latency: Optional[int] = Field(None, exclude=True)

    def get_deduplication_key(self) -> str:
        return f"{self.protocol}:{self.host}:{self.port}:{self.uuid}"

    def to_uri(self) -> str:
        raise NotImplementedError

class VmessConfig(BaseConfig):
    protocol: str = 'vmess'
    source_type: str = 'vmess'
    ps: str
    add: str
    v: Any = "2"
    aid: int = 0
    scy: str = 'auto'
    net: str
    type: str = 'none'
    tls: str = ''

    @model_validator(mode='before')
    def map_fields(cls, values):
        values['remarks'] = values.get('ps', 'N/A')
        values['host'] = values.get('add', '')
        values['uuid'] = values.get('id', '')
        values['network'] = values.get('net', 'tcp')
        values['security'] = values.get('tls') or 'none'
        values['v'] = str(values.get('v', '2'))
        return values

    def to_uri(self) -> str:
        vmess_data = {"v": self.v, "ps": self.remarks, "add": self.host, "port": self.port, "id": self.uuid, "aid": self.aid, "scy": self.scy, "net": self.network, "type": self.type, "host": self.sni, "path": self.path, "tls": self.security if self.security != 'none' else '', "sni": self.sni}
        vmess_data_clean = {k: v for k, v in vmess_data.items() if v is not None and v != ""}
        json_str = json.dumps(vmess_data_clean, separators=(',', ':'))
        encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8').rstrip("=")
        return f"vmess://{encoded}"

class VlessConfig(BaseConfig):
    protocol: str = 'vless'
    flow: Optional[str] = None
    pbk: Optional[str] = None
    sid: Optional[str] = None

    def to_uri(self) -> str:
        params = {'type': self.network, 'security': self.security, 'path': self.path, 'sni': self.sni, 'fp': self.fingerprint, 'flow': self.flow, 'pbk': self.pbk, 'sid': self.sid}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None and v != ""])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"vless://{self.uuid}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class TrojanConfig(BaseConfig):
    protocol: str = 'trojan'
    source_type: str = 'trojan'

    def to_uri(self) -> str:
        params = {'sni': self.sni, 'peer': self.sni, 'security': self.security}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"trojan://{self.uuid}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class ShadowsocksConfig(BaseConfig):
    protocol: str = 'shadowsocks'
    source_type: str = 'shadowsocks'
    method: str

    @model_validator(mode='before')
    def map_fields(cls, values):
        values['uuid'] = values.get('password', '')
        return values

    def to_uri(self) -> str:
        user_info = f"{self.method}:{self.uuid}"
        encoded_user_info = base64.b64encode(user_info.encode('utf-8')).decode('utf-8').rstrip('=')
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"ss://{encoded_user_info}@{self.host}:{self.port}{remarks_encoded}"

# ------------------------------------------------------------------------------
# --- FILENAME: network/http_client.py ---
# ------------------------------------------------------------------------------

class AsyncHttpClient:
    _client: Optional[httpx.AsyncClient] = None

    @classmethod
    async def get_client(cls) -> httpx.AsyncClient:
        if cls._client is None or cls._client.is_closed:
            limits = httpx.Limits(max_connections=CONFIG.MAX_CONCURRENT_REQUESTS, max_keepalive_connections=20)
            cls._client = httpx.AsyncClient(headers=CONFIG.HTTP_HEADERS, timeout=CONFIG.HTTP_TIMEOUT, max_redirects=CONFIG.HTTP_MAX_REDIRECTS, http2=True, follow_redirects=True, limits=limits)
        return cls._client

    @classmethod
    async def close(cls):
        if cls._client and not cls._client.is_closed:
            await cls._client.aclose()
            cls._client = None

    @classmethod
    async def get(cls, url: str) -> Tuple[int, str]:
        client = await cls.get_client()
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.status_code, response.text
        except httpx.RequestError as e:
            logger.error(f"HTTP request failed for {url}: {type(e).__name__}")
            raise NetworkError(f"Failed to fetch {url}") from e
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP status error for {url}: {e.response.status_code}")
            return e.response.status_code, e.response.text

# ------------------------------------------------------------------------------
# --- FILENAME: processing/parser.py ---
# ------------------------------------------------------------------------------

class V2RayParser:
    @staticmethod
    def parse(uri: str, source_type: str = "unknown") -> Optional[BaseConfig]:
        uri = uri.strip()
        parsed_config: Optional[BaseConfig] = None
        try:
            if uri.startswith("vmess://"): parsed_config = V2RayParser._parse_vmess(uri)
            elif uri.startswith("vless://"): parsed_config = V2RayParser._parse_vless(uri)
            elif uri.startswith("trojan://"): parsed_config = V2RayParser._parse_trojan(uri)
            elif uri.startswith("ss://"): parsed_config = V2RayParser._parse_shadowsocks(uri)

            if parsed_config:
                parsed_config.source_type = source_type
            return parsed_config
        except Exception as e:
            logger.error(f"Failed to parse URI: {uri[:60]}... | Error: {e}")
            return None

    @staticmethod
    def _parse_vmess(uri: str) -> Optional[VmessConfig]:
        b64_data = uri[len("vmess://"):]
        if not is_valid_base64(b64_data): return None
        data = json.loads(base64.b64decode(b64_data + '==').decode('utf-8'))
        return VmessConfig(**data)

    @staticmethod
    def _parse_vless(uri: str) -> Optional[VlessConfig]:
        try:
            parsed_url = urlparse(uri)
            port = parsed_url.port
            if port is None:
                match = re.search(r":(\d+)$", parsed_url.netloc)
                if match:
                    port = int(match.group(1))
                else:
                    logger.warning(f"Skipping VLESS config due to missing port: {uri[:60]}...")
                    return None

            params = parse_qs(parsed_url.query)
            return VlessConfig(uuid=parsed_url.username, host=parsed_url.hostname, port=port, remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{port}", network=params.get('type', ['tcp'])[0], security=params.get('security', ['none'])[0], path=unquote(params.get('path', [None])[0]) if params.get('path') else None, sni=params.get('sni', [None])[0], fingerprint=params.get('fp', [None])[0], flow=params.get('flow', [None])[0], pbk=params.get('pbk', [None])[0], sid=params.get('sid', [None])[0])
        except Exception as e:
            logger.warning(f"Could not parse VLESS link correctly: {uri[:60]}... | Error: {e}")
            return None

    @staticmethod
    def _parse_trojan(uri: str) -> Optional[TrojanConfig]:
        parsed_url = urlparse(uri)
        params = parse_qs(parsed_url.query)
        return TrojanConfig(uuid=parsed_url.username, host=parsed_url.hostname, port=parsed_url.port, remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}", security=params.get('security', ['tls'])[0], sni=params.get('sni', [None])[0], network='tcp')

    @staticmethod
    def _parse_shadowsocks(uri: str) -> Optional[ShadowsocksConfig]:
        try:
            main_part, remarks_part = (uri[len("ss://"):].split('#', 1) + [''])[:2]
            remarks = unquote(remarks_part) if remarks_part else ''
            user_info_part, host_port_part = main_part.split('@', 1)
            decoded_user_info = base64.b64decode(user_info_part + '==').decode('utf-8')
            method, password = decoded_user_info.split(':', 1)
            host, port_str = host_port_part.rsplit(':', 1)
            if host.startswith('[') and host.endswith(']'): host = host[1:-1]
            if not remarks: remarks = f"{host}:{port_str}"
            return ShadowsocksConfig(host=host, port=int(port_str), remarks=remarks, method=method, password=password)
        except Exception as e:
            logger.warning(f"Could not parse Shadowsocks link: {uri[:60]}... | Error: {e}")
            return None

# ------------------------------------------------------------------------------
# --- FILENAME: sources/raw_collector.py ---
# ------------------------------------------------------------------------------

class RawConfigCollector:
    PATTERNS = {"ss": r"(?<![\w-])(ss://[^\s<>#]+)", "trojan": r"(?<![\w-])(trojan://[^\s<>#]+)", "vmess": r"(?<![\w-])(vmess://[^\s<>#]+)", "vless": r"(?<![\w-])(vless://(?:(?!=reality)[^\s<>#])+(?=[\s<>#]))", "reality": r"(?<![\w-])(vless://[^\s<>#]+?security=reality[^\s<>#]*)"}

    @classmethod
    def find_all(cls, text_content: str) -> Dict[str, List[str]]:
        all_matches = {}
        for name, pattern in cls.PATTERNS.items():
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            cleaned_matches = [re.sub(r"#[^#]*$", "", m) for m in matches if "…" not in m]
            if cleaned_matches:
                all_matches[name] = cleaned_matches
        return all_matches

# ------------------------------------------------------------------------------
# --- FILENAME: sources/telegram_scraper.py ---
# ------------------------------------------------------------------------------

class TelegramScraper:
    def __init__(self, channels: List[str], since_datetime: datetime):
        self.channels, self.since_datetime, self.iran_tz = channels, since_datetime, get_iran_timezone()

    async def scrape_all(self) -> Tuple[Dict[str, List[str]], List[str]]:
        total_configs_by_type: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}

        batch_size = 20
        channel_batches = [self.channels[i:i + batch_size] for i in range(0, len(self.channels), batch_size)]

        total_channels = len(self.channels)
        logger.info(f"Starting to scrape {total_channels} channels in {len(channel_batches)} batches.")

        successful_channels: List[Tuple[str, int]] = []
        failed_channels: List[str] = []

        for i, batch in enumerate(channel_batches):
            logger.info(f"Processing batch {i+1}/{len(channel_batches)}...")
            tasks = [self._scrape_channel_with_retry(ch) for ch in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for j, channel_results in enumerate(results):
                channel_name = batch[j]
                if isinstance(channel_results, dict):
                    configs_found = sum(len(v) for v in channel_results.values())
                    if configs_found > 0:
                        successful_channels.append((channel_name, configs_found))
                        for config_type, configs in channel_results.items():
                            total_configs_by_type[config_type].extend(configs)
                else:
                    failed_channels.append(channel_name)
                    logger.warning(f"Failed to scrape channel '{channel_name}' after multiple retries.")

            success_count = sum(1 for res in results if isinstance(res, dict))
            failure_count = len(results) - success_count
            logger.info(f"Finished batch {i+1}/{len(channel_batches)}. Successes in this batch: {success_count}, Failures: {failure_count}")


            if i < len(channel_batches) - 1:
                sleep_duration = random.uniform(5, 10)
                logger.info(f"Cooling down for {sleep_duration:.2f} seconds before next batch...")
                await asyncio.sleep(sleep_duration)

        await self._write_scrape_report(successful_channels, failed_channels)
        return total_configs_by_type, failed_channels

    async def _write_scrape_report(self, successful: List[Tuple[str, int]], failed: List[str]):
        """Writes a summary of the scraping process to a report file."""
        now = datetime.now(get_iran_timezone())
        report_str = f"--- Telegram Scrape Report ---\n"
        report_str += f"Timestamp: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        report_str += f"Total Channels: {len(self.channels)}\n"
        report_str += f"Successful Scrapes: {len(successful)}\n"
        report_str += f"Failed Scrapes: {len(failed)}\n\n"

        report_str += "--- Channels with Found Configs ---\n"
        for channel, count in sorted(successful, key=lambda item: item[1], reverse=True):
            report_str += f"{channel}: {count} configs\n"

        report_str += "\n--- Failed Channels ---\n"
        for channel in sorted(failed):
            report_str += f"{channel}\n"

        try:
            async with aiofiles.open(CONFIG.TELEGRAM_REPORT_FILE, "w", encoding='utf-8') as f:
                await f.write(report_str)
            logger.info(f"Telegram scrape report saved to '{CONFIG.TELEGRAM_REPORT_FILE}'.")
        except IOError as e:
            logger.error(f"Could not write to Telegram report file: {e}")

    async def _scrape_channel_with_retry(self, channel: str, max_retries: int = 3) -> Optional[Dict[str, List[str]]]:
        """Scrapes a single channel with a retry mechanism for better stability."""
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                url = CONFIG.TELEGRAM_BASE_URL.format(channel)

                status, html = await AsyncHttpClient.get(url)
                if status == 200:
                    soup = BeautifulSoup(html, "html.parser")
                    # Use the configurable message limit
                    messages = soup.find_all("div", class_="tgme_widget_message", limit=CONFIG.TELEGRAM_MESSAGE_LIMIT)

                    if not messages:
                        return {}

                    channel_configs: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}

                    for msg in messages:
                        time_tag = msg.find("time", class_="time")
                        if time_tag and 'datetime' in time_tag.attrs:
                            try:
                                message_dt = datetime.fromisoformat(time_tag['datetime']).astimezone(self.iran_tz)
                                if message_dt > self.since_datetime:
                                    text_div = msg.find("div", class_="tgme_widget_message_text")
                                    if text_div:
                                        found_configs = RawConfigCollector.find_all(text_div.get_text('\n', strip=True))
                                        for config_type, configs in found_configs.items():
                                            channel_configs[config_type].extend(configs)
                            except (ValueError, TypeError):
                                continue
                    return channel_configs
                else:
                    logger.warning(f"[Attempt {attempt+1}/{max_retries}] Channel '{channel}' returned status {status}.")

            except NetworkError as e:
                logger.warning(f"[Attempt {attempt+1}/{max_retries}] Network error for channel '{channel}': {e}")
            except Exception as e:
                logger.error(f"[Attempt {attempt+1}/{max_retries}] Unexpected error for channel '{channel}': {e}")

            if attempt < max_retries - 1:
                sleep_duration = (attempt + 1) * 5
                logger.info(f"Retrying channel '{channel}' after {sleep_duration} seconds...")
                await asyncio.sleep(sleep_duration)

        return None


# ------------------------------------------------------------------------------
# --- FILENAME: sources/subscription_fetcher.py ---
# ------------------------------------------------------------------------------

class SubscriptionFetcher:
    def __init__(self, sub_links: List[str]): self.sub_links = sub_links

    async def fetch_all(self) -> Dict[str, List[str]]:
        tasks = [self._fetch_and_decode(link) for link in self.sub_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_configs_by_type: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}

        for content in results:
            if isinstance(content, str):
                found_configs = RawConfigCollector.find_all(content)
                for config_type, configs in found_configs.items():
                    total_configs_by_type[config_type].extend(configs)

        logger.info(f"Fetched {sum(len(v) for v in total_configs_by_type.values())} total configs from {len(self.sub_links)} subscriptions.")
        return total_configs_by_type

    async def _fetch_and_decode(self, link: str) -> str:
        try:
            _, content = await AsyncHttpClient.get(link)
            # Try decoding, if it fails, assume it's plain text
            try:
                decoded_content = base64.b64decode(content + '==').decode('utf-8')
                return decoded_content
            except Exception:
                return content
        except Exception:
             # If fetching fails, return empty string
            logger.error(f"Failed to fetch or decode subscription link: {link}")
            return ""


# ------------------------------------------------------------------------------
# --- FILENAME: storage/file_manager.py ---
# ------------------------------------------------------------------------------

class FileManager:
    def __init__(self, config: AppConfig):
        self.config = config
        self._setup_directories()

    def _setup_directories(self):
        self.config.OUTPUT_DIR.mkdir(exist_ok=True)
        for path in self.config.DIRS.values():
            path.mkdir(parents=True, exist_ok=True)

    async def read_json_file(self, file_path: Path) -> List[Any]:
        if not file_path.exists(): return []
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                return json.loads(await f.read())
        except Exception as e:
            logger.error(f"Error reading JSON file {file_path}: {e}")
            return []

    async def write_configs_to_file(self, file_path: Path, configs: List[BaseConfig], base64_encode: bool = True):
        if not configs: return
        final_list = self._add_signatures(configs) if CONFIG.ADD_SIGNATURES else [c.to_uri() for c in configs]
        content = "\n".join(final_list)
        if base64_encode: content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f: await f.write(content)
        except IOError as e:
            logger.error(f"Could not write to file {file_path}: {e}")

    def _add_signatures(self, configs: List[BaseConfig]) -> List[str]:
        uris = [c.to_uri() for c in configs]
        now = datetime.now(get_iran_timezone())
        update_str = f"[ LAST UPDATE: {now.strftime('%Y-%m-%d | %H:%M')} ]"

        final_list = uris[:]
        final_list.insert(0, self._create_title_config(update_str, 1080))
        final_list.insert(1, self._create_title_config(CONFIG.ADV_SIGNATURE, 2080))
        final_list.insert(2, self._create_title_config(CONFIG.DNT_SIGNATURE, 3080))
        final_list.insert(3, self._create_title_config(CONFIG.CUSTOM_SIGNATURE, 4080)) # NEW: Added custom signature
        final_list.append(self._create_title_config(CONFIG.DEV_SIGNATURE, 8080))
        return final_list

    def _create_title_config(self, title: str, port: int) -> str:
        return f"trojan://{generate_random_uuid_string()}@127.0.0.1:{port}?security=tls&type=tcp#{unquote(title)}"

# ------------------------------------------------------------------------------
# --- FILENAME: processing/processor.py ---
# ------------------------------------------------------------------------------

class Geolocation:
    _reader: Optional[geoip2.database.Reader] = None
    _ip_cache: Dict[str, Optional[str]] = {}

    @classmethod
    def initialize(cls):
        """Initializes the GeoIP database reader."""
        if not CONFIG.GEOIP_DB_FILE.exists():
            logger.error(f"GeoIP database not found at '{CONFIG.GEOIP_DB_FILE}'. Country detection will be skipped.")
            return
        try:
            cls._reader = geoip2.database.Reader(str(CONFIG.GEOIP_DB_FILE))
            logger.info("GeoIP database loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load GeoIP database: {e}")
            cls._reader = None

    @classmethod
    async def get_ip(cls, hostname: str) -> Optional[str]:
        """Resolves a hostname to an IP address asynchronously, with caching."""
        if hostname in cls._ip_cache:
            return cls._ip_cache[hostname]
        if is_ip_address(hostname):
            cls._ip_cache[hostname] = hostname
            return hostname
        try:
            loop = asyncio.get_running_loop()
            addr_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            ip = addr_info[0][4][0]
            cls._ip_cache[hostname] = ip
            return ip
        except Exception as e:
            logger.debug(f"Could not resolve hostname '{hostname}': {e}")
            cls._ip_cache[hostname] = None
            return None

    @classmethod
    def get_country_from_ip(cls, ip: str) -> str:
        """Looks up the country code for a given IP address."""
        if cls._reader is None or ip is None:
            return "XX"
        try:
            response = cls._reader.country(ip)
            return response.country.iso_code or "XX"
        except geoip2.errors.AddressNotFoundError:
            return "XX"
        except Exception:
            return "XX"

    @classmethod
    def close(cls):
        if cls._reader:
            cls._reader.close()

class ConfigProcessor:
    def __init__(self, raw_configs_by_type: Dict[str, List[str]]):
        self.raw_configs_by_type = raw_configs_by_type
        self.parsed_configs: Dict[str, BaseConfig] = {}
        self.total_raw_count = sum(len(v) for v in raw_configs_by_type.values())

    async def process(self):
        logger.info(f"Processing {self.total_raw_count} raw config strings.")

        all_parsed_configs: List[BaseConfig] = []
        for config_type, configs in self.raw_configs_by_type.items():
            for uri in configs:
                parsed = V2RayParser.parse(uri, source_type=config_type)
                if parsed:
                    all_parsed_configs.append(parsed)

        logger.info(f"Successfully parsed {len(all_parsed_configs)} configs.")

        for config in all_parsed_configs:
            key = config.get_deduplication_key()
            if key not in self.parsed_configs:
                self.parsed_configs[key] = config
        logger.info(f"Deduplication by URI resulted in {len(self.parsed_configs)} unique configs.")

        await self._resolve_countries()
        if CONFIG.ENABLE_IP_DEDUPLICATION:
            self._deduplicate_by_ip()

        if CONFIG.ENABLE_LATENCY_TEST:
            await self._test_latency()
            
        self._format_config_remarks()
        
        # NEW: Sort configs by latency (best first) | جدید: مرتب‌سازی بر اساس سرعت
        if CONFIG.ENABLE_LATENCY_TEST:
            self.parsed_configs = dict(sorted(self.parsed_configs.items(), key=lambda item: item[1].latency if item[1].latency is not None else 9999))


    async def _resolve_countries(self):
        unique_hosts = list({c.host for c in self.parsed_configs.values()})
        logger.info(f"Resolving IPs for {len(unique_hosts)} unique hosts...")
        await asyncio.gather(*[Geolocation.get_ip(host) for host in unique_hosts])

        logger.info("Looking up countries from local GeoIP database...")
        for config in self.parsed_configs.values():
            ip_address = Geolocation._ip_cache.get(config.host)
            config.country = Geolocation.get_country_from_ip(ip_address) if ip_address else "XX"

        resolved_count = sum(1 for c in self.parsed_configs.values() if c.country != "XX")
        logger.info(f"Successfully assigned countries to {resolved_count} configs.")

    def _deduplicate_by_ip(self):
        """Removes configs that resolve to the same IP, keeping only the first one."""
        logger.info("Deduplicating configs based on resolved IP address...")
        unique_ips: Dict[str, BaseConfig] = {}
        kept_configs: Dict[str, BaseConfig] = {}
        
        for key, config in self.parsed_configs.items():
            ip = Geolocation._ip_cache.get(config.host)
            if ip:
                if ip not in unique_ips:
                    unique_ips[ip] = config
                    kept_configs[key] = config
            else: # Keep configs whose hosts couldn't be resolved
                kept_configs[key] = config

        removed_count = len(self.parsed_configs) - len(kept_configs)
        self.parsed_configs = kept_configs
        logger.info(f"IP-based deduplication removed {removed_count} configs. {len(self.parsed_configs)} configs remaining.")

    async def _test_latency_for_config(self, config: BaseConfig) -> Optional[int]:
        """
        NEW: Tests latency for a single config.
        جدید: این تابع سرعت یک کانفیگ را تست می‌کند.
        """
        ip = Geolocation._ip_cache.get(config.host)
        if not ip:
            return None
        
        try:
            start_time = asyncio.get_event_loop().time()
            # Set a timeout for the connection attempt
            fut = asyncio.open_connection(ip, config.port)
            reader, writer = await asyncio.wait_for(fut, timeout=CONFIG.LATENCY_TIMEOUT)
            
            end_time = asyncio.get_event_loop().time()
            writer.close()
            await writer.wait_closed()
            
            # Latency in milliseconds
            latency = int((end_time - start_time) * 1000)
            return latency
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            logger.debug(f"Latency test failed for {ip}:{config.port} - {type(e).__name__}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during latency test for {ip}:{config.port}: {e}")
            return None


    async def _test_latency(self):
        """
        NEW: Runs latency tests on a subset of configs.
        جدید: این تابع تست سرعت را روی تعدادی از کانفیگ‌ها اجرا می‌کند.
        """
        logger.info("Starting latency testing for available configs...")
        configs_to_test = list(self.parsed_configs.values())
        if len(configs_to_test) > CONFIG.MAX_LATENCY_TESTS:
            logger.info(f"Testing a random sample of {CONFIG.MAX_LATENCY_TESTS} configs for latency.")
            configs_to_test = random.sample(configs_to_test, CONFIG.MAX_LATENCY_TESTS)
        else:
            logger.info(f"Testing all {len(configs_to_test)} configs for latency.")

        tasks = [self._test_latency_for_config(config) for config in configs_to_test]
        results = await asyncio.gather(*tasks)

        tested_count = 0
        successful_count = 0
        for config, latency in zip(configs_to_test, results):
            tested_count +=1
            if latency is not None:
                config.latency = latency
                successful_count +=1

        logger.info(f"Latency testing complete. {successful_count}/{len(configs_to_test)} configs responded successfully.")


    def _format_config_remarks(self):
        logger.info("Formatting remarks for all unique configs...")
        for config in self.parsed_configs.values():
            proto_full_map = {'vmess': 'VMESS', 'vless': 'VLESS', 'trojan': 'TROJAN', 'shadowsocks': 'SHADOWSOCKS'}
            proto_full = proto_full_map.get(config.protocol, 'CFG')

            sec = 'RLT' if config.source_type == 'reality' else (config.security.upper() if config.security != 'none' else 'NTLS')
            net = config.network.upper()
            flag = COUNTRY_CODE_TO_FLAG.get(config.country, "🏳️")
            ip_address = Geolocation._ip_cache.get(config.host, config.host)
            
            # NEW: Add latency to remarks if available | جدید: افزودن سرعت به نام کانفیگ
            latency_str = f"{config.latency}ms ┇ " if config.latency is not None else ""

            new_remark = f"{latency_str}{config.country} {flag} ┇ {proto_full}-{net}-{sec} ┇ {ip_address}"
            config.remarks = new_remark.strip()

    def get_all_unique_configs(self) -> List[BaseConfig]:
        return list(self.parsed_configs.values())

    def categorize(self) -> Dict[str, Dict[str, List[BaseConfig]]]:
        configs = self.get_all_unique_configs()
        categories: Dict[str, Dict[str, List[BaseConfig]]] = { "protocols": {}, "networks": {}, "security": {}, "countries": {} }
        for config in configs:
            if config.source_type == 'reality':
                categories["networks"].setdefault('reality', []).append(config)
            else:
                categories["protocols"].setdefault(config.protocol, []).append(config)
                categories["networks"].setdefault(config.network, []).append(config)
                if config.security not in ['none', 'reality']:
                    categories["security"].setdefault(config.security, []).append(config)

            if config.country and config.country != "XX":
                categories["countries"].setdefault(config.country, []).append(config)
        return categories

# ------------------------------------------------------------------------------
# --- FILENAME: main.py ---
# ------------------------------------------------------------------------------

class V2RayCollectorApp:
    def __init__(self):
        self.config = CONFIG
        self.file_manager = FileManager(self.config)
        self.last_update_time = datetime.now(get_iran_timezone()) - timedelta(days=1)

    async def run(self):
        logger.info("=" * 60)
        logger.info(" V2Ray Config Collector - @oxnet_ir Final Edition ".center(60, "="))
        logger.info("=".center(60, "="))

        await self._load_state()

        tg_channels = await self._get_telegram_channels()
        sub_links = await self.file_manager.read_json_file(self.config.SUBSCRIPTION_LINKS_FILE)

        tasks_to_run = []
        if tg_channels:
            tg_scraper = TelegramScraper(tg_channels, self.last_update_time)
            tasks_to_run.append(tg_scraper.scrape_all())
        else:
            # Provide a compatible empty result if no channels
            tasks_to_run.append(asyncio.sleep(0, result=({}, []))) 

        if sub_links and CONFIG.ENABLE_SUBSCRIPTION_FETCHING:
            sub_fetcher = SubscriptionFetcher(sub_links)
            tasks_to_run.append(sub_fetcher.fetch_all())
        else:
            # Provide a compatible empty result if no subs
            tasks_to_run.append(asyncio.sleep(0, result={}))

        if not tasks_to_run:
            logger.error("No sources (Telegram channels or subscriptions) to process. Exiting.")
            return

        results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        tg_result = results[0]
        sub_result = results[1]

        tg_raw_configs = tg_result[0] if isinstance(tg_result, tuple) and len(tg_result) > 0 else {}
        sub_raw_configs = sub_result if isinstance(sub_result, dict) else {}
        
        combined_raw_configs: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}
        for config_type in combined_raw_configs.keys():
            combined_raw_configs[config_type].extend(tg_raw_configs.get(config_type, []))
            combined_raw_configs[config_type].extend(sub_raw_configs.get(config_type, []))

        if not any(combined_raw_configs.values()):
            logger.info("No new configurations found from any source. Exiting.")
            return

        processor = ConfigProcessor(combined_raw_configs)
        await processor.process()

        all_unique_configs = processor.get_all_unique_configs()
        if not all_unique_configs:
            logger.info("No valid unique configurations to save. Exiting.")
            return

        categories = processor.categorize()
        await self._save_results(all_unique_configs, categories)
        await self._save_state()
        
        # NEW: Print final summary report | جدید: نمایش گزارش نهایی
        self._print_summary_report(processor)
        
        logger.info("Collection and processing complete.")

    async def _get_telegram_channels(self) -> List[str]:
        try:
            status, content = await AsyncHttpClient.get(self.config.REMOTE_CHANNELS_URL)
            if status == 200 and content:
                channels = json.loads(content)
                if isinstance(channels, list):
                    logger.info(f"Fetched {len(channels)} channels from remote source.")
                    return channels
        except (NetworkError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to fetch remote channel list: {e}. Falling back to local.")
        return await self.file_manager.read_json_file(self.config.TELEGRAM_CHANNELS_FILE)

    async def _load_state(self):
        if self.config.LAST_UPDATE_FILE.exists():
            try:
                async with aiofiles.open(self.config.LAST_UPDATE_FILE, 'r') as f:
                    self.last_update_time = datetime.fromisoformat(await f.read())
                    logger.info(f"Last run: {self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logger.error(f"Could not read last update file: {e}")

    async def _save_state(self):
        try:
            async with aiofiles.open(self.config.LAST_UPDATE_FILE, 'w') as f:
                await f.write(datetime.now(get_iran_timezone()).isoformat())
        except IOError as e:
            logger.error(f"Failed to save last update time: {e}")

    async def _save_results(self, all_configs: List[BaseConfig], categories: Dict[str, Any]):
        logger.info("Saving categorized configurations...")
        save_tasks: List[Coroutine] = []

        save_tasks.append(self.file_manager.write_configs_to_file(self.config.DIRS["subscribe"] / "base64.txt", all_configs))
        save_tasks.append(self.file_manager.write_configs_to_file(self.config.OUTPUT_DIR / "all_configs.txt", all_configs, base64_encode=False))

        for cat_name, cat_items in categories.items():
            for item_name, configs in cat_items.items():
                if configs:
                    path = self.config.DIRS[cat_name] / f"{item_name}.txt"
                    save_tasks.append(self.file_manager.write_configs_to_file(path, configs, base64_encode=False))

        chunk_size = math.ceil(len(all_configs) / 10) if all_configs else 0
        if chunk_size > 0:
            for i, chunk in enumerate([all_configs[i:i + chunk_size] for i in range(0, len(all_configs), chunk_size)]):
                path = self.config.DIRS["splitted"] / f"mixed_{i+1}.txt"
                save_tasks.append(self.file_manager.write_configs_to_file(path, chunk, base64_encode=False))

        await asyncio.gather(*save_tasks)
        logger.info(f"All files have been saved.")

    def _print_summary_report(self, processor: ConfigProcessor):
        """
        NEW: Prints a detailed summary of the collection process.
        جدید: این تابع یک گزارش کامل از فرآیند جمع‌آوری چاپ می‌کند.
        """
        all_configs = processor.get_all_unique_configs()
        
        protocol_counts = Counter(c.protocol for c in all_configs)
        country_counts = Counter(c.country for c in all_configs if c.country and c.country != 'XX')
        
        print("\n" + "="*60)
        print("📊 " + "Final Collection Report".center(56) + "📊")
        print("="*60)
        
        print(f"[*] Raw Configs Found:      {processor.total_raw_count}")
        print(f"[*] Unique & Valid Configs: {len(all_configs)}")
        
        if CONFIG.ENABLE_LATENCY_TEST:
            responsive_configs = sum(1 for c in all_configs if c.latency is not None)
            print(f"[*] Responsive (Pinged):    {responsive_configs}")
        
        print("-" * 60)
        print("📈 " + "Configs by Protocol:".ljust(56))
        for protocol, count in protocol_counts.most_common():
            print(f"    - {protocol.upper():<12}: {count}")
        
        print("-" * 60)
        print("🌍 " + "Top 5 Countries:".ljust(56))
        for country_code, count in country_counts.most_common(5):
            flag = COUNTRY_CODE_TO_FLAG.get(country_code, '🏳️')
            print(f"    {flag} - {country_code:<12}: {count}")
            
        print("=" * 60)
        
# --- Main Execution ---
async def main():
    # Ensure the main data directory exists
    CONFIG.DATA_DIR.mkdir(exist_ok=True)

    # Download GeoIP database if it doesn't exist
    if not CONFIG.GEOIP_DB_FILE.exists():
        logger.info("GeoLite2-Country.mmdb not found, downloading...")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(CONFIG.GEOIP_DB_URL, follow_redirects=True, timeout=120.0)
                response.raise_for_status()
                async with aiofiles.open(CONFIG.GEOIP_DB_FILE, "wb") as f:
                    await f.write(response.content)
                logger.info("GeoLite2-Country.mmdb downloaded successfully.")
        except Exception as e:
            logger.critical(f"Failed to download GeoLite2 database: {e}. Country detection will be unreliable.")

    Geolocation.initialize()

    # Create default subscription links file if it doesn't exist, with new links
    if not CONFIG.SUBSCRIPTION_LINKS_FILE.exists():
        new_links = [
            "https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/splitted/mixed",
            "https://raw.githubusercontent.com/miladtahanian/V2RayCFGDumper/main/config.txt",
            "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/main/all_configs.txt",
            "https://raw.githubusercontent.com/V2RAYCONFIGSPOOL/V2RAY_SUB/main/v2ray_configs.txt",
            "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/all_sub.txt",
            "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/sub_merge.txt",
            "https://raw.githubusercontent.com/MrMohebi/xray-proxy-grabber-telegram/master/collected-proxies/row-url/all.txt",
            "https://raw.githubusercontent.com/MrMohebi/xray-proxy-grabber-telegram/master/collected-proxies/row-url/actives.txt",
            "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/main/full/5ubscrpt10n.txt",
            "https://raw.githubusercontent.com/skywrt/v2ray-configs/main/All_Configs_Sub.txt",
            "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt",
            "https://raw.githubusercontent.com/Kwinshadow/TelegramV2rayCollector/main/sublinks/mix.txt",
            "https://raw.githubusercontent.com/GuoBing1989100/v2ray_configs/main/all.txt",
            "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/main/mix/sub.html",
            "https://raw.githubusercontent.com/hamed1124/port-based-v2ray-configs/main/All-Configs.txt",
            "https://raw.githubusercontent.com/miladesign/TelegramV2rayCollector/main/api/normal",
            "https://raw.githubusercontent.com/SamanGho/v2ray_collector/main/v2tel_links1.txt",
            "https://raw.githubusercontent.com/jagger235711/V2rayCollector/main/results/mixed_tested.txt",
            "https://raw.githubusercontent.com/SamanGho/v2ray_collector/main/v2tel_links2.txt",
            "https://raw.githubusercontent.com/nyeinkokoaung404/V2ray-Configs/main/All_Configs_Sub.txt",
            "https://raw.githubusercontent.com/Epodonios/bulk-xray-v2ray-vless-vmess-trojan-ss-configs/main/sub/Iran/config.txt",
            "https://raw.githubusercontent.com/Surfboardv2ray/TGParse/main/configtg.txt"
        ]
        with open(CONFIG.SUBSCRIPTION_LINKS_FILE, "w") as f:
            json.dump(list(set(new_links)), f, indent=4)

    # Create default channels file if it doesn't exist from the remote URL
    if not CONFIG.TELEGRAM_CHANNELS_FILE.exists():
         try:
            status, content = await AsyncHttpClient.get(CONFIG.REMOTE_CHANNELS_URL)
            if status == 200 and content:
                channels = json.loads(content)
                if isinstance(channels, list):
                    async with aiofiles.open(CONFIG.TELEGRAM_CHANNELS_FILE, "w", encoding='utf-8') as f:
                        await f.write(json.dumps(channels, indent=4))
                    logger.info(f"Default telegram_channels.json created from remote source.")
         except Exception as e:
            logger.warning(f"Could not create default telegram channels file: {e}")


    app = V2RayCollectorApp()
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user.")
    except Exception as e:
        logger.critical(f"An unhandled exception occurred: {e}", exc_info=True)
    finally:
        await AsyncHttpClient.close()
        Geolocation.close()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
