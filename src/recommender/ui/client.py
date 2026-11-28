"""Giới hạn thời gian gọi HTTP; giao diện chỉ giao tiếp với API."""

import httpx


class ApiError(RuntimeError):
    def __init__(
        self, message: str, *, status_code: int | None = None, code: str | None = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code

    @property
    def retryable(self) -> bool:
        return (
            self.status_code is None
            or self.status_code >= 500
            or self.status_code in {408, 429}
        )


class SnapshotChanged(ApiError):
    """Thông tin sản phẩm gợi ý được yêu cầu cho lần chạy không còn hoạt động."""


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
            exception = (
                SnapshotChanged if "X-Active-Run-ID" in response.headers else ApiError
            )
            raise exception(
                detail.get("message", "API không xử lý được yêu cầu"),
                status_code=response.status_code,
                code=detail.get("code"),
            )
        return data

    def products(self, offset=0):
        return self.request(
            "GET", "/api/v1/products", params={"offset": offset, "limit": 12}
        )["items"]

    def product(self, product_id, *, run_id=None):
        from urllib.parse import quote

        headers = {"X-Expected-Run-ID": run_id} if run_id else {}
        return self.request(
            "GET", "/api/v1/products/" + quote(product_id, safe=""), headers=headers
        )

    def recommendation_products(self, fetch):
        """Lấy cả nhóm theo mã lần chạy đã khai báo; tải lại một lần nếu snapshot đổi."""
        for _ in range(2):
            response = fetch()
            try:
                products = [
                    (row, self.product(row["product_id"], run_id=response["run_id"]))
                    for row in response["items"]
                ]
                return response, products
            except SnapshotChanged:
                continue
        raise SnapshotChanged("Danh mục đang cập nhật. Hãy tải lại gợi ý.")

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
