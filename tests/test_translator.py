"""Tests for MalloyTranslator."""

from pathlib import Path

from dagster import AssetKey

from dagster_malloy.parser import MalloyParsedModel, MalloyQueryInfo, MalloySourceInfo
from dagster_malloy.translator import MalloyTranslator, MalloyTranslatorData


def test_default_translator_spec():
    file_path = Path("analytics/sales.malloy")
    q_info = MalloyQueryInfo(
        name="monthly_revenue",
        source_name="orders",
        description="Monthly revenue query",
        tags=["financial"],
    )
    parsed = MalloyParsedModel(file_path=file_path)
    trans_data = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed,
        file_path=file_path,
        table_dependencies={"orders.parquet"},
        compiled_sql="SELECT sum(amount) FROM orders",
        dialect="duckdb",
    )

    translator = MalloyTranslator()
    spec = translator.get_asset_spec(trans_data)

    assert spec.kinds == {"🔍  query", "malloy", "duckdb"}
    assert spec.key == AssetKey(["sales", "monthly_revenue"])
    assert spec.description == "Monthly revenue query"
    assert spec.group_name == "malloy"
    assert spec.tags["dagster-malloy/file"] == "sales.malloy"
    assert spec.tags["malloy/financial"] == "true"
    assert spec.metadata["compiled_sql"] == "SELECT sum(amount) FROM orders"
    assert spec.metadata["dialect"] == "duckdb"

    # Upstream dependencies should include sales/orders when include_sources=True
    dep_keys = [dep.asset_key for dep in spec.deps]
    assert AssetKey(["sales", "orders"]) in dep_keys

    trans_data_no_src = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed,
        file_path=file_path,
        table_dependencies={"orders.parquet"},
        include_sources=False,
    )
    spec_no_src = translator.get_asset_spec(trans_data_no_src)
    dep_keys_no_src = [dep.asset_key for dep in spec_no_src.deps]
    assert AssetKey("orders") in dep_keys_no_src


class CustomTranslator(MalloyTranslator):
    def get_asset_key(self, data: MalloyTranslatorData) -> AssetKey:
        return AssetKey(["custom_namespace", data.query_info.name])

    def get_group_name(self, data: MalloyTranslatorData) -> str:
        return "custom_malloy_group"


def test_custom_translator():
    file_path = Path("sales.malloy")
    q_info = MalloyQueryInfo(name="q1")
    parsed = MalloyParsedModel(file_path=file_path)
    trans_data = MalloyTranslatorData(
        query_info=q_info,
        parsed_model=parsed,
        file_path=file_path,
    )

    translator = CustomTranslator()
    spec = translator.get_asset_spec(trans_data)

    assert spec.key == AssetKey(["custom_namespace", "q1"])
    assert spec.group_name == "custom_malloy_group"


def test_source_extension_lineage():
    file_path = Path("analytics/comments.malloy")
    parsed = MalloyParsedModel(
        file_path=file_path,
        sources={
            "comments_base": MalloySourceInfo(name="comments_base", table_or_sql="comments.parquet"),
            "comments": MalloySourceInfo(name="comments", base_source_name="comments_base"),
        },
    )

    translator = MalloyTranslator()
    spec_base = translator.get_source_asset_spec("comments_base", file_path, parsed)
    spec_ext = translator.get_source_asset_spec("comments", file_path, parsed)

    assert [dep.asset_key for dep in spec_base.deps] == [AssetKey("comments")]
    assert [dep.asset_key for dep in spec_ext.deps] == [AssetKey(["comments", "comments_base"])]
