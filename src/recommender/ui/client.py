"""Bounded HTTP calls; UI communicates with the API only."""

import httpx


class ApiError(RuntimeError):
    pass


class ApiClient:
    def __init__(self, base_url: str, timeout: float = 5):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, **kwargs):
        try:
            with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
                response = client.request(method, path, **kwargs)
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ApiError(
                "Không thể kết nối API. Thử lại khi dịch vụ sẵn sàng."
            ) from exc
        if response.is_error:
            detail = data.get("detail", {}) if isinstance(data, dict) else {}
            raise ApiError(detail.get("message", "API không xử lý được yêu cầu"))
        return data

    def products(self, offset=0):
        return self.request(
            "GET", "/api/v1/products", params={"offset": offset, "limit": 12}
        )["items"]

    def product(self, product_id):
        from urllib.parse import quote

        return self.request("GET", "/api/v1/products/" + quote(product_id, safe=""))

    def personalized(self, user_id, session_id=None):
        params = {"user_id": user_id, "limit": 6}
        if session_id:
            params["session_id"] = session_id
        return self.request(
            "GET", "/api/v1/recommendations/personalized", params=params
        )

    def substitutes(self, product_id):
        return self.request(
            "GET",
            "/api/v1/recommendations/substitutes",
            params={"product_id": product_id, "limit": 6},
        )

    def complements(self, cart):
        return self.request(
            "POST",
            "/api/v1/recommendations/complements",
            json={"product_ids": sorted(cart), "limit": 6},
        )
