import io

import pandas as pd
import pytest

import cleaner as c


def write(tmp_path, text, name="data.csv", encoding="utf-8"):
    p = tmp_path / name
    p.write_bytes(text.encode(encoding))
    return str(p)


# ---------------------------------------------------------------- Phase 1

def test_utf8_file_is_untouched(tmp_path):
    path = write(tmp_path, "name\ncafé\n")
    out, enc, stats = c.handle_encoding_issues(path, return_stats=True)
    assert out == path and enc == "utf-8" and stats["converted"] is False


def test_non_utf8_is_converted(tmp_path):
    path = write(tmp_path, "name,city\n" + "José,Zürich\n" * 50, encoding="cp1252")
    out, _, stats = c.handle_encoding_issues(path, return_stats=True)
    assert stats["converted"] and out.endswith("_utf8.csv")
    with open(out, encoding="utf-8") as f:
        assert "José" in f.read()


def test_non_utf8_abort(tmp_path):
    path = write(tmp_path, "name\n" + "José\n" * 50, encoding="cp1252")
    with pytest.raises(c.UserAbort):
        c.handle_encoding_issues(path, on_non_utf8="abort")


def test_trailing_comma_on_header_and_rows(tmp_path):
    path = write(tmp_path, "a,b,\n1,2,\n3,4,\n")
    buf, source, stats = c.handle_trailing_commas_preload(path, return_stats=True)
    assert source == "memory" and stats["header_flagged"] and stats["data_rows_flagged"] == 2
    assert buf.getvalue().splitlines() == ["a,b", "1,2", "3,4"]


def test_empty_last_column_is_not_a_trailing_comma(tmp_path):
    # Regression: legitimately empty last values used to be stripped.
    path = write(tmp_path, "a,b,c\n1,2,\n3,4,5\n")
    data, source, stats = c.handle_trailing_commas_preload(path, return_stats=True)
    assert source == "file" and stats["data_rows_flagged"] == 0
    assert c.validate_column_counts_preload(data) is True


def test_trailing_comma_save_policy(tmp_path):
    path = write(tmp_path, "a,b,\n1,2,\n")
    out, source = c.handle_trailing_commas_preload(path, on_trailing="save")
    assert source == "file" and out.endswith("_corrected.csv")


def test_structural_error_reports_rows():
    buf = io.StringIO("a,b\n1,2\n1,2,3\n4\n")
    with pytest.raises(c.StructuralError) as exc:
        c.validate_column_counts_preload(buf)
    assert [r["row_number"] for r in exc.value.rows] == [3, 4]


def test_load_keeps_leading_zeros_and_nulls():
    df = c.load_csv(io.StringIO("zip,v\n00501,NULL\n02134,n/a\n"))
    assert df["zip"].tolist() == ["00501", "02134"]
    assert df["v"].isna().all()


def test_load_header_only_raises():
    with pytest.raises(ValueError):
        c.load_csv(io.StringIO("a,b\n"))


def test_merged_headers_are_dropped():
    df = pd.DataFrame({"name": ["Name", "ann", "NAME"], "age": ["age", "3", "Age"]})
    out, stats = c.handle_merged_headers(df, return_stats=True)
    assert stats["header_rows"] == [0, 2]
    assert out["name"].tolist() == ["ann"]


def test_merged_headers_abort():
    df = pd.DataFrame({"name": ["name"], "age": ["age"]})
    with pytest.raises(c.UserAbort):
        c.handle_merged_headers(df, on_detected="abort")


def test_postload_flags_only_real_unnamed_columns():
    df = pd.DataFrame({"Unnamed: 2": [1], "unnamed_entity": [2]})
    ok, stats = c.validate_column_counts_postload(df, return_stats=True)
    assert not ok and stats["unnamed_columns"] == ["Unnamed: 2"]


def test_drop_empty_rows():
    df = pd.DataFrame({"a": [1, None], "b": ["x", None]})
    assert len(c.drop_empty_rows(df)) == 1


# ---------------------------------------------------------------- Phase 2

def test_empty_column_names_are_named():
    df = pd.DataFrame([[1, 2, 3]], columns=[" a ", "Unnamed: 1", "  "])
    out = c.handle_empty_column_names(df)
    assert list(out.columns) == ["a", "col_1", "col_2"]


def test_special_chars_become_underscores():
    df = pd.DataFrame(columns=["First Name", "Price ($)", "Café"])
    out = c.handle_special_characters_in_headers(df)
    assert list(out.columns) == ["First_Name", "Price", "Cafe"]


@pytest.mark.parametrize("raw,expected", [
    ("customerID", "customer_id"),
    ("HTTPResponseCode", "http_response_code"),
    ("First Name", "first_name"),
    ("already_snake", "already_snake"),
    ("2024 Sales", "col_2024_sales"),
    ("!!!", "col"),
])
def test_to_snake_case(raw, expected):
    assert c.to_snake_case(raw) == expected


def test_duplicates_get_suffixes():
    df = pd.DataFrame([[1, 2, 3, 4]], columns=["a", "a", "a_2", "a"])
    out = c.handle_duplicate_column_names(df)
    assert list(out.columns) == ["a", "a_3", "a_2", "a_4"]


def test_duplicates_drop():
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    out = c.handle_duplicate_column_names(df, on_duplicate="drop")
    assert list(out.columns) == ["a"] and out.iloc[0, 0] == 1


def test_invalid_policy_raises():
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    with pytest.raises(ValueError):
        c.handle_duplicate_column_names(df, on_duplicate="nope")


def test_interactive_prompt_reprompts_on_bad_input(monkeypatch):
    answers = iter(["9", "x", "2"])
    monkeypatch.setattr("builtins.input", lambda _="": next(answers))
    df = pd.DataFrame({"name": ["name", "ann"]})
    out = c.handle_merged_headers(df, interactive=True)  # option 2 = keep
    assert len(out) == 2


# ---------------------------------------------------------------- Phase 3

def test_null_like_strings_with_whitespace():
    df = pd.DataFrame({"a": [" N/A ", "x", "--", None]})
    out, stats = c.handle_null_like_strings(df, return_stats=True)
    assert stats == {"a": 2}
    assert out["a"].isna().tolist() == [True, False, True, True]


def test_null_like_with_duplicate_index():
    df = pd.DataFrame({"a": ["null", "keep"]}, index=[0, 0])
    out = c.handle_null_like_strings(df)
    assert out["a"].isna().tolist() == [True, False]


def test_strip_string_values():
    df = pd.DataFrame({"a": ["  x ", "y", None]})
    assert c.strip_string_values(df)["a"].tolist()[:2] == ["x", "y"]


def test_infer_types():
    df = pd.DataFrame({
        "flag": ["Yes", "no", None],
        "qty": ["0", "1", "1"],
        "n": ["1", "2", None],
        "price": ["1.5", " 2 ", "3"],
        "zip": ["00501", "12345", "99999"],
        "text": ["a", "1", "2"],
    }, dtype=object)
    out, conv = c.infer_column_types(df, return_stats=True)
    assert str(out["flag"].dtype) == "boolean"
    assert str(out["qty"].dtype) == "Int64"          # 0/1 is not a bool by default
    assert str(out["n"].dtype) == "Int64" and out["n"].isna().sum() == 1
    assert str(out["price"].dtype) == "float64"
    assert "zip" not in conv and "text" not in conv


def test_numeric_bools_opt_in():
    df = pd.DataFrame({"qty": ["0", "1"]}, dtype=object)
    out = c.infer_column_types(df, numeric_bools=True)
    assert str(out["qty"].dtype) == "boolean"


# ---------------------------------------------------------------- Pipeline

MESSY = (
    "Customer ID,First Name,First-Name,Zip Code,Active,Amount ($),\n"
    "Customer ID,First Name,First-Name,Zip Code,Active,Amount ($),\n"
    "1, Ann ,A,00501,yes,10.50,\n"
    "2,Bob,B,02134,NO, N/A ,\n"
    ",,,,,,\n"
    "3,Cy,C,90210,Yes,7,\n"
)


def test_full_pipeline(tmp_path):
    path = write(tmp_path, MESSY)
    df, report = c.clean_csv(path)
    assert list(df.columns) == [
        "customer_id", "first_name", "first_name_2", "zip_code", "active", "amount",
    ]
    assert len(df) == 3
    assert df["first_name"].tolist() == ["Ann", "Bob", "Cy"]
    assert df["zip_code"].tolist() == ["00501", "02134", "90210"]
    assert str(df["active"].dtype) == "boolean"
    assert str(df["customer_id"].dtype) == "Int64"
    assert df["amount"].isna().sum() == 1
    assert report["merged_headers"]["header_rows"] == [0]


def test_cli_writes_output(tmp_path, capsys):
    path = write(tmp_path, MESSY)
    out = tmp_path / "out.csv"
    assert c.main([path, "-o", str(out)]) == 0
    assert out.read_text(encoding="utf-8").startswith("customer_id,first_name")


def test_cli_returns_error_code_on_structural_issue(tmp_path):
    path = write(tmp_path, "a,b\n1,2,3,4\n")
    assert c.main([path]) == 1
