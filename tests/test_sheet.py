import polars as pl

from quicksql.sheet import Sheet

FRAME = pl.DataFrame(
    {
        "name": ["ada", "bob", "cyd", "bob"],
        "score": [3, 1, 2, 1],
        "active": [True, False, True, True],
    }
)


def test_cursor_moves_and_clamps() -> None:
    sheet = Sheet(FRAME)
    assert sheet.move(-1, -1).cursor == (0, 0)
    assert sheet.move(2, 1).cursor == (2, 1)
    assert sheet.move(99, 99).cursor == (3, 2)
    assert sheet.bottom().cursor[0] == 3
    assert sheet.bottom().top().cursor[0] == 0


def test_sort_by_cursor_column() -> None:
    sheet = Sheet(FRAME).move(0, 1)  # cursor on "score"
    assert sheet.sort().frame["score"].to_list() == [1, 1, 2, 3]
    assert sheet.sort(desc=True).frame["score"].to_list() == [3, 2, 1, 1]


def test_hide_column() -> None:
    sheet = Sheet(FRAME).move(0, 1).hide_current()
    assert sheet.columns == ["name", "active"]
    assert "score" not in sheet.visible().columns
    assert sheet.cursor[1] <= len(sheet.columns) - 1


def test_select_rows() -> None:
    sheet = Sheet(FRAME).toggle_select().move(1, 0).toggle_select()
    assert sheet.selected == {0, 1}
    assert sheet.toggle_select().selected == {0}
    assert sheet.select_all().selected == {0, 1, 2, 3}
    assert sheet.select_all().select_all().selected == set()


def test_frequency_of_cursor_column() -> None:
    freq = Sheet(FRAME).freq()  # cursor on "name"
    assert freq.frame.columns == ["name", "count", "share"]
    top = freq.frame.row(0, named=True)
    assert top["name"] == "bob" and top["count"] == 2 and abs(top["share"] - 0.5) < 1e-9
    assert freq.title.startswith("freq")


def test_describe_pushes_summary() -> None:
    described = Sheet(FRAME).describe()
    assert "statistic" in described.frame.columns
    assert described.frame.height > 0


def test_search_wraps_and_moves_cursor() -> None:
    sheet = Sheet(FRAME)
    hit = sheet.search("bob")
    assert hit.cursor[0] == 1
    again = hit.search("bob")
    assert again.cursor[0] == 3
    wrapped = again.search("bob")
    assert wrapped.cursor[0] == 1
    missing = sheet.search("zelda")
    assert missing.cursor == sheet.cursor


def test_drill_payload_survives_ops_but_not_derived_sheets() -> None:
    marker = object()
    sheet = Sheet(FRAME, drill=marker)
    assert sheet.move(1, 1).drill is marker
    assert sheet.sort().drill is marker
    assert sheet.move(0, 1).hide_current().drill is marker
    assert sheet.toggle_select().drill is marker
    assert sheet.search("bob").drill is marker
    # derived sheets are new data, not the drillable listing
    assert sheet.freq().drill is None
    assert sheet.describe().drill is None


def test_filtered_matches_regex_across_visible_columns() -> None:
    sheet = Sheet(FRAME)
    hit = sheet.filtered("^b.b$")
    assert hit.frame["name"].to_list() == ["bob", "bob"]
    assert hit.title == "filter(^b.b$)"
    # case-insensitive, and non-string columns match on their repr
    assert sheet.filtered("ADA").frame.height == 1
    assert sheet.filtered("true").frame.height == 3
    # a hidden column no longer matches
    assert sheet.move(0, 1).hide_current().filtered("^3$").frame.height == 0
    assert sheet.filtered("^3$").frame.height == 1


def test_filtered_keeps_drill_and_resets_cursor() -> None:
    marker = object()
    sheet = Sheet(FRAME, drill=marker).move(3, 1)
    hit = sheet.filtered("bob")
    assert hit.drill is marker  # a filtered listing still drills
    assert hit.cursor == (0, 1)  # row cursor back on top, column kept


def test_filtered_invalid_or_empty_pattern_returns_self() -> None:
    sheet = Sheet(FRAME)
    assert sheet.filtered("[unclosed") is sheet
    assert sheet.filtered("") is sheet


def test_a_sample_carries_through_every_derived_sheet() -> None:
    # a sheet over an output's first rows: whatever is computed from it is too
    s = Sheet(pl.DataFrame({"a": [1, 2, 2], "b": ["x", "y", "z"]}), sample=3)
    derived = [s.sort(), s.freq(), s.describe(), s.filtered("y"), s.hide_current(), s.move(1, 0)]
    assert [d.sample for d in derived] == [3] * len(derived)
    assert Sheet(pl.DataFrame({"a": [1]})).freq().sample is None
