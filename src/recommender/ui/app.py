"""Streamlit storefront: streamlit run src/recommender/ui/app.py."""

import io

import httpx
import streamlit as st
from PIL import Image, UnidentifiedImageError
from pydantic import TypeAdapter, ValidationError

from recommender.config import get_settings
from recommender.schemas import BusinessId
from recommender.ui.client import ApiClient, ApiError
from recommender.ui.state import StoreState

st.set_page_config(page_title="Gợi ý sản phẩm", page_icon="🛍️", layout="wide")
st.title("Khám phá sản phẩm phù hợp")
st.caption("Gợi ý theo sở thích, sản phẩm đang xem và những món trong giỏ của bạn.")
client = ApiClient(get_settings().api_base_url)


@st.cache_data(ttl=3600, max_entries=128, show_spinner=False)
def image_bytes(url):
    if not url:
        return None
    try:
        with httpx.stream("GET", url, timeout=2, follow_redirects=True) as response:
            response.raise_for_status()
            result = bytearray()
            for chunk in response.iter_bytes():
                result.extend(chunk)
                if len(result) > 5_000_000:
                    return None
        Image.open(io.BytesIO(result)).verify()
        return bytes(result)
    except (
        httpx.HTTPError,
        UnidentifiedImageError,
        OSError,
        Image.DecompressionBombError,
    ):
        return None


if "store" not in st.session_state:
    st.session_state.store = StoreState("u1")
state = st.session_state.store
user = st.sidebar.text_input("Người dùng", value="u1", key="user-id")
try:
    TypeAdapter(BusinessId).validate_python(user)
except ValidationError:
    st.error("Nhập mã người dùng hợp lệ, tối đa 128 ký tự.")
    st.stop()
state.change_user(user)
use_session = st.sidebar.toggle("Cập nhật theo phiên", value=True)
st.sidebar.caption("Bật để gợi ý phản hồi ngay sau thao tác xem và chọn sản phẩm.")
st.sidebar.metric("Sản phẩm trong giỏ", len(state.cart))


def flush():
    try:
        state.flush(client)
    except (ApiError, ValueError) as exc:
        st.warning(f"{exc} Thao tác được giữ lại để gửi lại.")


if state.pending:
    st.sidebar.warning(f"{len(state.pending)} sự kiện đang chờ gửi")
    if st.sidebar.button("Gửi lại sự kiện"):
        flush()
        st.rerun()


def open_product(product_id):
    state.open_product(product_id)
    flush()
    st.rerun()


def suggestions(title, response, key):
    st.subheader(title)
    if not response["items"]:
        st.info("Chưa có gợi ý phù hợp trong ngữ cảnh này.")
        return
    st.caption(f"Phiên bản gợi ý: {response['run_id']}")
    for row in response["items"]:
        try:
            product = client.product(row["product_id"])
        except ApiError:
            st.info("Danh mục vừa thay đổi. Tải lại gợi ý để tiếp tục.")
            continue
        if st.button(
            product["name"], key=f"{key}-{row['product_id']}", width="stretch"
        ):
            open_product(row["product_id"])
        st.caption(product["category_id"])


try:
    catalog_tab, detail_tab, cart_tab = st.tabs(["Danh mục", "Đang xem", "Giỏ hàng"])
    with catalog_tab:
        products = client.products(state.page * 12)
        cols = st.columns(3)
        for index, product in enumerate(products):
            with cols[index % 3], st.container(border=True):
                st.markdown("### 🛍️ " + product["name"])
                st.caption(product["category_id"])
                if st.button("Xem sản phẩm", key="catalog-" + product["product_id"]):
                    open_product(product["product_id"])
        previous, next_page = st.columns(2)
        if previous.button("Trang trước", disabled=state.page == 0):
            state.page -= 1
            st.rerun()
        if next_page.button("Trang sau", disabled=len(products) < 12):
            state.page += 1
            st.rerun()
        suggestions(
            "Dành cho bạn",
            client.personalized(
                state.user_id, state.session_id if use_session else None
            ),
            "personalized",
        )
    with detail_tab:
        if state.selected:
            product = client.product(state.selected)
            st.subheader(product["name"])
            image = image_bytes(product["image_url"])
            if image:
                st.image(image, width=260)
            else:
                st.info("🛍️ Chưa có ảnh sản phẩm")
            st.write(product["description"] or "Chưa có mô tả.")
            if st.button("Thêm vào giỏ", key="add-cart", type="primary"):
                state.add_to_cart()
                flush()
                st.rerun()
            suggestions(
                "Sản phẩm thay thế", client.substitutes(state.selected), "substitute"
            )
        else:
            st.info(
                "Chọn một sản phẩm trong danh mục để xem chi tiết và sản phẩm thay thế."
            )
    with cart_tab:
        if state.cart:
            for product_id in sorted(state.cart):
                try:
                    st.write("🛍️ " + client.product(product_id)["name"])
                except ApiError:
                    st.write(product_id + " — sản phẩm đã đổi phiên bản")
            if st.button("Xóa giỏ hàng"):
                state.cart.clear()
                st.rerun()
        else:
            st.info("Giỏ hàng đang trống.")
        suggestions("Thường thêm cùng", client.complements(state.cart), "complement")
        st.caption("Các gợi ý dựa trên sản phẩm thường xuất hiện cùng giỏ.")
except ApiError as exc:
    st.error(str(exc))
    if st.button("Tải lại"):
        st.rerun()
