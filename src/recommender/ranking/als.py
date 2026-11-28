"""T06 lọc cộng tác bằng Implicit ALS (PySpark ML).

* Ánh xạ ID chuỗi -> chỉ số int32 theo thứ tự ID tăng dần; song ánh và
  có thể tái lập (DATA_CONTRACTS.md mục 4.4).
* Dùng ``implicitPrefs=True``, lấy ``interaction_weight`` làm đầu vào độ
  tin cậy và ``coldStartStrategy='drop'``; người dùng/sản phẩm không có
  nhân tố dùng cơ chế dự phòng popularity chung.
* Lấy top ``CANDIDATE_LIMIT`` mỗi người dùng sau khi loại sản phẩm đã xem,
  sản phẩm ngoài danh mục lần chạy và điểm không hữu hạn; nếu bằng điểm
  thì xếp theo ID sản phẩm.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from recommender.ranking.common import CANDIDATE_LIMIT, Scored

if TYPE_CHECKING:
    from pyspark.ml.recommendation import ALSModel
    from pyspark.sql import DataFrame, SparkSession

MAX_INDEX = 2_147_483_647


@dataclass(frozen=True, slots=True)
class AlsParams:
    """Siêu tham số của một cấu hình ALS (EXPERIMENT_PLAN.md mục 6)."""

    rank: int = 32
    max_iter: int = 10
    reg_param: float = 0.1
    alpha: float = 10.0
    seed: int = 42

    def as_dict(self) -> dict[str, float | int]:
        """Biểu diễn thuận tiện để ghi JSON trong manifest và báo cáo."""
        return asdict(self)


ALS_GRID = tuple(
    AlsParams(rank=rank, reg_param=reg, alpha=alpha)
    for rank in (16, 32)
    for reg in (0.05, 0.1)
    for alpha in (1.0, 10.0)
)


def build_index(df: DataFrame, id_col: str, index_col: str) -> DataFrame:
    """Ánh xạ song ánh ``(id, int index)`` theo thứ tự ID tăng dần."""
    from pyspark.sql import types as T

    ids = df.select(id_col).distinct().orderBy(id_col)
    mapped = ids.rdd.zipWithIndex().map(lambda pair: (pair[0][0], int(pair[1])))
    schema = T.StructType(
        [
            T.StructField(id_col, T.StringType(), False),
            T.StructField(index_col, T.IntegerType(), False),
        ]
    )
    out = df.sparkSession.createDataFrame(mapped, schema)
    return out


@dataclass(slots=True)
class AlsArtifacts:
    """Mô hình đã huấn luyện kèm đúng các ánh xạ ID dùng khi học."""

    model: ALSModel
    user_index: DataFrame
    product_index: DataFrame
    params: AlsParams


def fit_als(interactions: DataFrame, params: AlsParams) -> AlsArtifacts:
    """Huấn luyện implicit ALS trên tương tác Gold của một cửa sổ học."""
    from pyspark.ml.recommendation import ALS

    user_index = build_index(interactions, "user_id", "user_index").cache()
    product_index = build_index(interactions, "product_id", "product_index").cache()
    if user_index.count() > MAX_INDEX or product_index.count() > MAX_INDEX:
        raise ValueError("ALS index space exceeds int32")
    ratings = (
        interactions.join(user_index, "user_id")
        .join(product_index, "product_id")
        .select("user_index", "product_index", "interaction_weight")
    )
    als = ALS(
        rank=params.rank,
        maxIter=params.max_iter,
        regParam=params.reg_param,
        alpha=params.alpha,
        implicitPrefs=True,
        userCol="user_index",
        itemCol="product_index",
        ratingCol="interaction_weight",
        coldStartStrategy="drop",
        nonnegative=False,
        seed=params.seed,
    )
    return AlsArtifacts(als.fit(ratings), user_index, product_index, params)


def als_candidates(
    artifacts: AlsArtifacts,
    seen: DataFrame,
    catalog: DataFrame,
    *,
    limit: int = CANDIDATE_LIMIT,
) -> dict[str, list[Scored]]:
    """Ứng viên tốt nhất chưa xem trong danh mục của từng người dùng, dưới dạng danh sách trên driver.

    Lấy ``limit + số sản phẩm đã xem tối đa mỗi người dùng`` để bước lọc
    không làm thiếu gợi ý khi vẫn còn sản phẩm chưa xem có điểm dự đoán.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    max_seen = seen.groupBy("user_id").count().agg(F.max("count")).first()[0] or 0
    n_items = artifacts.product_index.count()
    fetch = int(min(n_items, limit + max_seen))
    if fetch == 0:
        return {}
    recs = (
        artifacts.model.recommendForAllUsers(fetch)
        .select("user_index", F.explode("recommendations").alias("r"))
        .select(
            "user_index", F.col("r.product_index"), F.col("r.rating").alias("score")
        )
        .join(artifacts.user_index, "user_index")
        .join(artifacts.product_index, "product_index")
        .join(catalog.select("product_id"), "product_id", "left_semi")
        .join(
            seen.select("user_id", "product_id"), ["user_id", "product_id"], "left_anti"
        )
        .where(~F.isnan("score") & (F.abs("score") < float("inf")))
    )
    order = Window.partitionBy("user_id").orderBy(F.col("score").desc(), "product_id")
    ranked = (
        recs.withColumn("rank", F.row_number().over(order))
        .where(F.col("rank") <= limit)
        .select("user_id", "product_id", "score", "rank")
        .collect()
    )
    result: dict[str, list[tuple[int, Scored]]] = {}
    for row in ranked:
        result.setdefault(row["user_id"], []).append(
            (row["rank"], Scored(row["product_id"], float(row["score"]), "als"))
        )
    return {user: [item for _, item in sorted(items)] for user, items in result.items()}


def save_als(
    artifacts: AlsArtifacts, model_uri: str, user_uri: str, product_uri: str
) -> None:
    """Lưu mô hình cùng đúng các ánh xạ ID đã dùng khi huấn luyện."""
    artifacts.model.write().overwrite().save(model_uri)
    artifacts.user_index.orderBy("user_index").coalesce(1).write.mode(
        "overwrite"
    ).parquet(user_uri)
    artifacts.product_index.orderBy("product_index").coalesce(1).write.mode(
        "overwrite"
    ).parquet(product_uri)


def load_als(
    spark: SparkSession,
    model_uri: str,
    user_uri: str,
    product_uri: str,
    params: AlsParams | None = None,
) -> AlsArtifacts:
    """Nạp lại mô hình đã lưu; từ chối ánh xạ không khớp các nhân tố của mô hình."""
    from pyspark.ml.recommendation import ALSModel

    model = ALSModel.load(model_uri)
    user_index = spark.read.parquet(user_uri).cache()
    product_index = spark.read.parquet(product_uri).cache()
    for factors, index, column in (
        (model.userFactors, user_index, "user_index"),
        (model.itemFactors, product_index, "product_index"),
    ):
        ids = {r["id"] for r in factors.select("id").collect()}
        mapped = {r[column] for r in index.select(column).collect()}
        if ids != mapped:
            raise ValueError(f"Saved {column} mapping does not match the model factors")
    return AlsArtifacts(
        model, user_index, product_index, params or AlsParams(rank=model.rank)
    )
