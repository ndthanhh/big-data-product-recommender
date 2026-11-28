"""T05 đặc trưng nội dung TF-IDF, hồ sơ nội dung người dùng và gợi ý TF-IDF cá nhân hóa.

Văn bản = ``name + category_id + description`` (null -> ""), chuẩn hóa
Unicode NFC và chữ thường. Token là chuỗi chữ/số dài nhất, giữ nguyên
từ tiếng Việt có dấu. Spark ML ``CountVectorizer`` + ``IDF`` chỉ học từ
danh mục có trước mốc thời gian (EXPERIMENT_PLAN.md mục 3.3 và 6).

Số sản phẩm trong danh mục nhỏ, nên vector sau khi học được chuẩn hóa L2
và đưa vào ma trận NumPy đặc trên driver; dữ liệu sự kiện vẫn ở Spark.
Hồ sơ người dùng là ``L2(sum_i weight(u,i) * v_i)`` và điểm là cosine.
"""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from recommender.ranking.common import CANDIDATE_LIMIT, Scored

if TYPE_CHECKING:
    from pyspark.ml import PipelineModel
    from pyspark.sql import DataFrame

TOKEN_PATTERN = r"[^\p{L}\p{N}]+"
VOCAB_SIZE = 20_000
MIN_DF = 1.0
USER_BATCH = 2048


def normalize_text(
    name: str | None, category_id: str | None, description: str | None
) -> str:
    """Ghép văn bản, chuẩn hóa NFC và chữ thường cho cả bước học và biến đổi."""
    text = " ".join(part or "" for part in (name, category_id, description))
    return unicodedata.normalize("NFC", text).lower()


@dataclass(slots=True)
class TfidfFeatures:
    """Các dòng TF-IDF chuẩn hóa L2 của danh mục thuộc một lần chạy."""

    product_ids: list[str]
    categories: list[str]
    matrix: np.ndarray  # (n_products, vocab), chuẩn hóa L2 hoặc toàn 0
    index: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.index:
            self.index = {pid: i for i, pid in enumerate(self.product_ids)}

    @property
    def zero_vectors(self) -> int:
        """Các sản phẩm có văn bản không tạo ra trọng số TF-IDF khác 0."""
        return int((np.linalg.norm(self.matrix, axis=1) == 0).sum())

    def vector(self, product_id: str) -> np.ndarray | None:
        """Vector đã chuẩn hóa, hoặc ``None`` khi thiếu hay toàn 0."""
        row = self.index.get(product_id)
        if row is None:
            return None
        vec = self.matrix[row]
        return vec if vec.any() else None


def fit_tfidf(
    catalog: DataFrame, *, vocab_size: int = VOCAB_SIZE, min_df: float = MIN_DF
) -> tuple[PipelineModel, TfidfFeatures]:
    """Huấn luyện CountVectorizer + IDF trên ``catalog`` và trả đặc trưng đã chuẩn hóa."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer
    from pyspark.sql import functions as F

    text = F.udf(normalize_text, "string")
    docs = catalog.select(
        "product_id",
        "category_id",
        text("name", "category_id", "description").alias("text"),
    )
    pipeline = Pipeline(
        stages=[
            RegexTokenizer(
                inputCol="text",
                outputCol="tokens",
                pattern=TOKEN_PATTERN,
                gaps=True,
                toLowercase=False,
                minTokenLength=1,
            ),
            CountVectorizer(
                inputCol="tokens", outputCol="tf", vocabSize=vocab_size, minDF=min_df
            ),
            IDF(inputCol="tf", outputCol="features"),
        ]
    )
    model = pipeline.fit(docs)
    rows = (
        model.transform(docs)
        .select("product_id", "category_id", "features")
        .orderBy("product_id")
        .collect()
    )
    width = len(model.stages[1].vocabulary)
    matrix = np.zeros((len(rows), max(width, 1)), dtype=np.float64)
    for i, row in enumerate(rows):
        vec = row["features"]
        matrix[i, vec.indices] = vec.values
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    np.divide(matrix, norms, out=matrix, where=norms > 0)
    features = TfidfFeatures(
        [r["product_id"] for r in rows], [r["category_id"] for r in rows], matrix
    )
    return model, features


def features_from_rows(
    rows: Iterable[tuple[str, str, list[int], list[float], int]],
) -> TfidfFeatures:
    """Dựng lại đặc trưng đã lưu bởi :func:`feature_rows` khi nạp lại."""
    rows = sorted(rows)
    width = max((r[4] for r in rows), default=1)
    matrix = np.zeros((len(rows), max(width, 1)), dtype=np.float64)
    for i, (_, _, indices, values, _) in enumerate(rows):
        matrix[i, indices] = values
    return TfidfFeatures([r[0] for r in rows], [r[1] for r in rows], matrix)


def feature_rows(
    features: TfidfFeatures,
) -> list[tuple[str, str, list[int], list[float], int]]:
    """Các dòng thưa theo sản phẩm để lưu vector chuẩn hóa của từng lần chạy."""
    out = []
    width = features.matrix.shape[1]
    for pid, category, vec in zip(
        features.product_ids, features.categories, features.matrix, strict=True
    ):
        nz = np.flatnonzero(vec)
        out.append((pid, category, nz.tolist(), vec[nz].tolist(), width))
    return out


def user_profiles(
    features: TfidfFeatures, interactions: Iterable[tuple[str, str, float]]
) -> tuple[list[str], np.ndarray]:
    """Hồ sơ có trọng số chuẩn hóa L2; bỏ qua người dùng có hồ sơ rỗng."""
    by_user: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for user_id, product_id, weight in interactions:
        row = features.index.get(product_id)
        if row is not None and weight > 0:
            by_user[user_id].append((row, float(weight)))
    users = sorted(by_user)
    profiles = np.zeros((len(users), features.matrix.shape[1]), dtype=np.float64)
    for u, user_id in enumerate(users):
        rows, weights = zip(*by_user[user_id], strict=True)
        profiles[u] = np.asarray(weights) @ features.matrix[list(rows)]
    norms = np.linalg.norm(profiles, axis=1)
    keep = norms > 0
    profiles = profiles[keep] / norms[keep, None]
    return [u for u, k in zip(users, keep, strict=True) if k], profiles


def _top_rows(
    scores: np.ndarray, product_ids: list[str], exclude: Iterable[int], limit: int
) -> list[tuple[str, float]]:
    scores = scores.copy()
    scores[list(exclude)] = -np.inf
    candidates = np.flatnonzero(scores > 0)
    if candidates.size == 0:
        return []
    if candidates.size > limit:
        # Giữ mọi sản phẩm bằng điểm với sản phẩm tốt thứ limit tại ranh giới chọn.
        part = np.argpartition(-scores[candidates], limit - 1)[:limit]
        threshold = scores[candidates[part]].min()
        candidates = candidates[scores[candidates] >= threshold]
    ordered = sorted(
        ((product_ids[i], float(min(1.0, scores[i]))) for i in candidates),
        key=lambda item: (-item[1], item[0]),
    )
    return ordered[:limit]


def recommend_tfidf(
    features: TfidfFeatures,
    interactions: Iterable[tuple[str, str, float]],
    seen: Mapping[str, set[str]],
    *,
    limit: int = CANDIDATE_LIMIT,
    users: Iterable[str] | None = None,
) -> dict[str, list[Scored]]:
    """TF-IDF cá nhân hóa: cosine giữa hồ sơ và sản phẩm chưa xem trong danh mục."""
    user_ids, profiles = user_profiles(features, interactions)
    wanted = None if users is None else set(users)
    result: dict[str, list[Scored]] = {}
    for start in range(0, len(user_ids), USER_BATCH):
        block = profiles[start : start + USER_BATCH] @ features.matrix.T
        for offset, scores in enumerate(block):
            user_id = user_ids[start + offset]
            if wanted is not None and user_id not in wanted:
                continue
            exclude = [
                features.index[p] for p in seen.get(user_id, ()) if p in features.index
            ]
            result[user_id] = [
                Scored(pid, score, "tfidf")
                for pid, score in _top_rows(
                    scores, features.product_ids, exclude, limit
                )
            ]
    return result
