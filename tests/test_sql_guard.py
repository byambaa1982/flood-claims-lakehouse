import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai.sql_guard import MAX_ROWS, UnsafeSQL, check  # noqa: E402


def test_select_is_wrapped_with_a_row_cap():
    result = check("select state, count(*) from nfip_claims group by 1;")
    assert result.startswith("select * from (")
    assert result.endswith(f"limit {MAX_ROWS}")
    assert "group by 1" in result
    assert ";" not in result


def test_cte_names_are_allowed_as_tables():
    check(
        "with by_state as (select state, sum(total_paid) paid from nfip_claims group by 1), "
        "top as (select * from by_state order by paid desc) select * from top"
    )


def test_qualified_table_name_is_allowed():
    check('select count(*) from flood_claims_lakehouse_curated."nfip_claims"')


@pytest.mark.parametrize(
    "sql",
    [
        "",
        "drop table nfip_claims",
        "delete from nfip_claims",
        "select 1; select 2",
        "select * from nfip_claims; drop table nfip_claims",
        "select * from information_schema.tables",
        "select * from nfip_claims join other_db.secrets on true",
        "with x as (select 1) insert into nfip_claims select * from x",
        "show tables",
        "select * from nfip_claims -- harmless\n; drop table nfip_claims",
    ],
)
def test_unsafe_statements_are_rejected(sql):
    with pytest.raises(UnsafeSQL):
        check(sql)


def test_extract_from_is_not_read_as_a_table():
    check("select extract(month from date_of_loss) m, count(*) from nfip_claims group by 1")


def test_keywords_inside_string_literals_do_not_trip_the_check():
    check("select count(*) from nfip_claims where flood_event = 'Drop; delete update'")
