"""
KIS API client for daily price data with BYMD backward pagination.
Adapted from asset_us kis_service.py
"""

import json
import time
import requests
from datetime import datetime
from pathlib import Path
from config.settings import Settings

TOKEN_CACHE_FILE = Path(__file__).resolve().parent.parent / ".token_cache.json"


class KISClient:

    def __init__(self):
        self.settings = Settings()
        self.base_url = self.settings.BASE_URL
        self.app_key = self.settings.APP_KEY
        self.app_secret = self.settings.SECRET_KEY
        self.cano = self.settings.CANO
        self.acnt_prdt_cd = self.settings.ACNT_PRDT_CD

        self._access_token = None
        self._token_expired = None
        self._last_call_time = 0
        self._min_interval = 0.5

        self._load_token_cache()

    def _load_token_cache(self):
        try:
            if TOKEN_CACHE_FILE.exists():
                with open(TOKEN_CACHE_FILE, "r") as f:
                    cache = json.load(f)
                    self._access_token = cache.get("access_token")
                    expired_str = cache.get("token_expired")
                    if expired_str:
                        self._token_expired = datetime.strptime(expired_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    def _save_token_cache(self):
        try:
            cache = {
                "access_token": self._access_token,
                "token_expired": self._token_expired.strftime("%Y-%m-%d %H:%M:%S") if self._token_expired else None,
            }
            with open(TOKEN_CACHE_FILE, "w") as f:
                json.dump(cache, f)
        except Exception:
            pass

    def _wait_for_rate_limit(self):
        elapsed = time.time() - self._last_call_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call_time = time.time()

    def get_access_token(self):
        if self._access_token and self._token_expired:
            if datetime.now() < self._token_expired:
                return self._access_token

        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"content-type": "application/json; charset=utf-8"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        self._wait_for_rate_limit()
        response = requests.post(url, headers=headers, data=json.dumps(body))

        if response.status_code == 403:
            if self._access_token:
                return self._access_token
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

        data = response.json()
        if "access_token" not in data:
            raise Exception(f"No access_token in response: {data}")

        self._access_token = data["access_token"]
        if "access_token_token_expired" in data:
            try:
                self._token_expired = datetime.strptime(
                    data["access_token_token_expired"], "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                self._token_expired = None

        self._save_token_cache()
        return self._access_token

    def _get_headers(self, tr_id):
        token = self.get_access_token()
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    def get_daily_prices(self, symbol, exchange_code="NAS", bymd="", adjust="1"):
        """단일 요청으로 ~100캔들 조회. output2 는 최신→과거 순."""
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        headers = self._get_headers("HHDFS76240000")
        params = {
            "AUTH": "",
            "EXCD": exchange_code,
            "SYMB": symbol,
            "GUBN": "0",
            "BYMD": bymd,
            "MODP": adjust,
        }

        self._wait_for_rate_limit()
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            return []

        data = response.json()
        if data.get("rt_cd") != "0":
            return []

        results = []
        for item in (data.get("output2") or []):
            xymd = item.get("xymd", "")
            if not xymd:
                continue
            clos = float(item.get("clos", 0))
            if clos <= 0:
                continue
            results.append({
                "date": xymd,
                "open": float(item.get("open", 0)),
                "high": float(item.get("high", 0)),
                "low": float(item.get("low", 0)),
                "close": clos,
                "volume": int(item.get("tvol", 0)),
            })
        return results

    def get_daily_prices_paginated(self, symbol, exchange_code="NAS", target_days=260):
        """BYMD 페이지네이션으로 target_days 만큼 일봉 수집. 최종 결과는 과거→최신 순."""
        all_results = []
        bymd = ""

        while len(all_results) < target_days:
            batch = self.get_daily_prices(symbol, exchange_code, bymd=bymd)
            if not batch:
                break

            all_results.extend(batch)

            # output2는 최신→과거 순이므로 마지막이 가장 오래된 날짜
            oldest_date = batch[-1]["date"]
            bymd = oldest_date

            if len(batch) < 90:
                break

        # 중복 제거 + 과거→최신 정렬
        seen = set()
        unique = []
        for r in all_results:
            if r["date"] not in seen:
                seen.add(r["date"])
                unique.append(r)
        unique.sort(key=lambda x: x["date"])
        return unique
