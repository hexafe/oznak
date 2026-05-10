import pytest

from oznak.errors import OznakValidationError
from oznak.filters import QueryFilter
from oznak.profiles import DatabaseProfile
from oznak.query_builder import QuerySpec, compile_query


def _profile(dialect: str = "mysql") -> DatabaseProfile:
    return DatabaseProfile(
        alias="assembly",
        dialect=dialect,
        host="db.example.invalid",
        port=3306,
        database="process_db",
        table="records",
        allowed_columns=("id", "reference", "status", "updated_at", "count", "is_valid"),
        timestamp_column="updated_at",
        pagination_column="id",
    )


def test_mysql_query_uses_backticks_bound_params_and_limit_order():
    compiled = compile_query(
        _profile("mysql"),
        QuerySpec(
            filters=(
                QueryFilter("reference", "=", "R1"),
                QueryFilter("count", ">=", 3),
                QueryFilter("is_valid", "=", False),
            ),
            columns=("reference", "count", "is_valid"),
            limit=50,
        ),
    )

    assert compiled.sql == (
        "SELECT `reference`, `count`, `is_valid` FROM `records` "
        "WHERE `reference` = :param_0 AND `count` >= :param_1 AND `is_valid` = :param_2 "
        "ORDER BY `updated_at` DESC LIMIT 50"
    )
    assert dict(compiled.params) == {"param_0": "R1", "param_1": 3, "param_2": False}


def test_mssql_query_uses_brackets_and_top_limit():
    compiled = compile_query(
        _profile("mssql"),
        QuerySpec(
            filters=(QueryFilter("status", "!=", "SCRAP"),),
            columns=("reference", "status"),
            limit=10,
            date_column="updated_at",
        ),
    )

    assert compiled.sql == (
        "SELECT TOP 10 [reference], [status] FROM [records] "
        "WHERE [status] != :param_0 ORDER BY [updated_at] DESC"
    )
    assert dict(compiled.params) == {"param_0": "SCRAP"}


def test_rejects_disallowed_selected_filter_date_and_pagination_columns():
    profile = _profile()

    with pytest.raises(OznakValidationError, match="operator"):
        compile_query(profile, QuerySpec(columns=("operator",)))

    with pytest.raises(OznakValidationError, match="operator"):
        compile_query(profile, QuerySpec(filters=(QueryFilter("operator", "=", "A"),)))

    with pytest.raises(OznakValidationError, match="operator"):
        compile_query(profile, QuerySpec(limit=1, date_column="operator"))

    with pytest.raises(OznakValidationError, match="operator"):
        compile_query(profile, QuerySpec(chunk_size=100, pagination_column="operator"))


def test_typed_null_predicates_render_without_params():
    compiled = compile_query(
        _profile(),
        QuerySpec(
            filters=(
                QueryFilter.is_null("updated_at"),
                QueryFilter.is_not_null("status"),
            ),
        ),
    )

    assert compiled.sql == "SELECT * FROM `records` WHERE `updated_at` IS NULL AND `status` IS NOT NULL"
    assert dict(compiled.params) == {}


def test_normal_null_like_string_remains_bound_param():
    compiled = compile_query(
        _profile(),
        QuerySpec(filters=(QueryFilter("status", "=", "NULL"),)),
    )

    assert compiled.sql == "SELECT * FROM `records` WHERE `status` = :param_0"
    assert dict(compiled.params) == {"param_0": "NULL"}


def test_in_with_comma_containing_values_uses_one_param_per_value():
    compiled = compile_query(
        _profile(),
        QuerySpec(filters=(QueryFilter("reference", "IN", ("A,1", "B,2")),)),
    )

    assert compiled.sql == "SELECT * FROM `records` WHERE `reference` IN (:param_0,:param_1)"
    assert dict(compiled.params) == {"param_0": "A,1", "param_1": "B,2"}


def test_not_in_rejects_empty_lists_via_query_filter_contract():
    with pytest.raises(OznakValidationError):
        QueryFilter("reference", "NOT IN", [])


@pytest.mark.parametrize("field_name", ("limit", "chunk_size"))
@pytest.mark.parametrize("bad_value", (0, -1, 1.5, "10", True))
def test_rejects_invalid_limit_and_chunk_size(field_name, bad_value):
    kwargs = {field_name: bad_value}

    with pytest.raises(OznakValidationError, match=field_name):
        compile_query(_profile(), QuerySpec(**kwargs))


def test_chunked_query_uses_pagination_column_and_chunk_size_by_dialect():
    mysql_compiled = compile_query(
        _profile("mysql"),
        QuerySpec(chunk_size=100, last_pagination_value=25),
    )
    mssql_compiled = compile_query(
        _profile("mssql"),
        QuerySpec(chunk_size=100, last_pagination_value=25),
    )

    assert mysql_compiled.sql == (
        "SELECT * FROM `records` WHERE `id` > :pagination_param ORDER BY `id` ASC LIMIT 100"
    )
    assert dict(mysql_compiled.params) == {"pagination_param": 25}
    assert mssql_compiled.sql == (
        "SELECT TOP 100 * FROM [records] WHERE [id] > :pagination_param ORDER BY [id] ASC"
    )
    assert dict(mssql_compiled.params) == {"pagination_param": 25}
