"""
cleaner.py
==========
A CSV cleaning pipeline for pandas DataFrames. It produces UTF-8 output
that is ready to import into SQL.

The pipeline runs in three phases:

    1. File ingestion: encoding, trailing commas, column counts, loading,
       merged headers, and blank rows.
    2. Header cleaning: empty names, special characters, snake_case,
       and duplicates.
    3. Value cleaning: whitespace, null-like strings, and type inference.

Every step that used to stop and ask a question takes a *policy* argument
instead, such as ``on_non_utf8="convert"``. That way the pipeline can run
unattended, in scripts, or in tests. Pass ``interactive=True`` to bring the
prompts back. In that mode the policy value becomes the default answer.

Messages go through the standard :mod:`logging` module. Warnings are always
shown. To also see progress messages, call
``logging.basicConfig(level=logging.INFO)`` or use the ``-v`` flag on the
command line.

Quick start::

    from cleaner import clean_csv
    df, report = clean_csv("data.csv")
    df.to_csv("data_clean.csv", index=False)

Or from the command line::

    python cleaner.py data.csv -o data_clean.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import sys
import unicodedata
from collections.abc import Sequence
from typing import Union

import chardet
import pandas as pd

__all__ = [
    "CleaningError",
    "StructuralError",
    "UserAbort",
    "clean_csv",
    "convert_headers_to_snake_case",
    "drop_empty_rows",
    "handle_duplicate_column_names",
    "handle_empty_column_names",
    "handle_encoding_issues",
    "handle_merged_headers",
    "handle_null_like_strings",
    "handle_special_characters_in_headers",
    "handle_trailing_commas_preload",
    "infer_column_types",
    "load_csv",
    "strip_string_values",
    "to_snake_case",
    "validate_column_counts_postload",
    "validate_column_counts_preload",
]

logger = logging.getLogger(__name__)

CsvSource = Union[str, "os.PathLike[str]", io.StringIO]

# ============================================================
# Exceptions
# ============================================================


class CleaningError(RuntimeError):
    """Base class for errors raised by this module."""


class UserAbort(CleaningError):
    """Raised when a policy is ``"abort"`` or the user chooses to abort."""


class StructuralError(CleaningError):
    """Raised when a CSV has rows with an inconsistent number of columns.

    Attributes:
        rows: One dict per bad row, with the keys ``row_number``,
            ``expected_columns``, ``actual_columns`` and ``row_preview``.
    """

    def __init__(self, message: str, rows: list[dict]):
        super().__init__(message)
        self.rows = rows


# ============================================================
# Module-level constants and compiled regexes
# ============================================================

_ACRONYM_RE = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_RE = re.compile(r"([a-z0-9])([A-Z])")
_NONWORD_RE = re.compile(r"[^\w]+")
_MULTI_US_RE = re.compile(r"_+")
_UNNAMED_RE = re.compile(r"^Unnamed: \d+(_level_\d+)?$")
_LEADING_ZERO_RE = re.compile(r"^[+-]?0\d")

# Compared case-insensitively, after stripping whitespace.
NULL_LIKE_VALUES = frozenset({
    "null", "n/a", "na", "nan", "none", "nil",
    "-", "--", "?", "??", "",
})

BOOL_MAP = {
    "true": True, "false": False,
    "yes": True, "no": False,
    "t": True, "f": False,
    "y": True, "n": False,
}
_NUMERIC_BOOL_MAP = {"1": True, "0": False}

_UTF8_COMPATIBLE = {"utf-8", "utf-8-sig", "ascii"}

_RULE = "=" * 60


# ============================================================
# Internal helpers
# ============================================================


def _require_df(df) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame, got {type(df).__name__}.")


def _require_file(path) -> None:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")


def _sibling_path(file_path: str, suffix: str) -> str:
    """Build a new path next to the source file, e.g. ``data.csv`` -> ``data_utf8.csv``."""
    root, ext = os.path.splitext(os.fspath(file_path))
    return f"{root}{suffix}{ext or '.csv'}"


def _decide(
    policy: str,
    options: dict[str, str],
    *,
    interactive: bool,
    question: str,
) -> str:
    """Work out what to do when an issue is detected.

    When ``interactive`` is false, the policy is checked and returned as is.
    When it is true, the user sees a numbered menu built from ``options``
    (a mapping of policy name to description). The current policy is the
    default answer, so pressing Enter accepts it. Invalid input asks again
    instead of raising. End-of-input or Ctrl-C counts as ``"abort"`` when
    that is one of the options.
    """
    if policy not in options:
        raise ValueError(
            f"Invalid policy {policy!r}. Expected one of: {', '.join(options)}."
        )
    if not interactive:
        return policy

    keys = list(options)
    print(f"\n{question}")
    for i, key in enumerate(keys, start=1):
        marker = " (default)" if key == policy else ""
        print(f"  {i}. {options[key]}{marker}")

    while True:
        try:
            raw = input(f"Enter choice [1-{len(keys)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            if "abort" in options:
                return "abort"
            raise
        if raw == "":
            return policy
        if raw.isdigit() and 1 <= int(raw) <= len(keys):
            return keys[int(raw) - 1]
        print(f"  Invalid choice {raw!r}. Please enter a number from 1 to {len(keys)}.")


def _open_source(data: CsvSource, encoding: str, errors: str):
    """Return a text stream for a path or buffer, plus whether we own (must close) it."""
    if isinstance(data, io.StringIO):
        data.seek(0)
        return data, False
    if isinstance(data, (str, os.PathLike)):
        _require_file(data)
        return open(data, encoding=encoding, errors=errors, newline=""), True
    raise TypeError(f"Expected a file path or io.StringIO, got {type(data).__name__}.")


def _ret(value, stats: dict, return_stats: bool):
    """Return ``value`` alone, or ``value`` and ``stats`` together when stats are requested."""
    if not return_stats:
        return value
    if isinstance(value, tuple):
        return (*value, stats)
    return value, stats


# ============================================================
# Phase 1: File ingestion
# ============================================================


def handle_encoding_issues(
    file_path: str,
    *,
    on_non_utf8: str = "convert",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Make sure a CSV file is UTF-8, converting a copy if it is not.

    The file is first decoded as UTF-8. That check is exact and fast, so
    chardet only runs on files that fail it. A file that is not UTF-8 is
    handled according to ``on_non_utf8``:

    * ``"convert"``: write ``<name>_utf8<ext>`` next to the original and
      use that file from then on. The original is never modified.
    * ``"abort"``: raise :class:`UserAbort`.

    Args:
        file_path: Path to the CSV file.
        on_non_utf8: Policy for files that are not UTF-8.
        interactive: Ask the user instead of applying the policy.
        return_stats: Also return a stats dict.

    Returns:
        ``(path, encoding)``, where ``path`` may point to the converted copy
        and ``encoding`` is always ``"utf-8"``. When ``return_stats`` is true,
        a third item is added: a dict with the keys ``detected_encoding``,
        ``confidence`` and ``converted``.

    Raises:
        FileNotFoundError: The file does not exist.
        UserAbort: The policy or the user chose to abort.
        CleaningError: The encoding could not be detected.
    """
    _require_file(file_path)

    with open(file_path, "rb") as f:
        raw = f.read()

    stats = {"detected_encoding": "utf-8", "confidence": 1.0, "converted": False}

    try:
        raw.decode("utf-8")
        logger.info("Encoding is UTF-8. No conversion needed.")
        return _ret((file_path, "utf-8"), stats, return_stats)
    except UnicodeDecodeError:
        pass

    result = chardet.detect(raw)
    detected = result.get("encoding")
    confidence = result.get("confidence") or 0.0
    stats.update(detected_encoding=detected, confidence=confidence)

    if not detected:
        raise CleaningError(
            f"Could not detect the encoding of {file_path}. "
            "Convert it to UTF-8 manually and reload."
        )
    if detected.lower() in _UTF8_COMPATIBLE:
        # chardet guessed UTF-8, but strict decoding already failed above, so
        # the file contains invalid byte sequences. Don't guess any further.
        raise CleaningError(
            f"{file_path} looks like UTF-8 but contains invalid byte sequences. "
            "Fix or re-export the file and reload."
        )

    logger.warning(
        "File encoding is %s (confidence %.0f%%), not UTF-8.", detected, confidence * 100
    )

    action = _decide(
        on_non_utf8,
        {
            "convert": "Convert to UTF-8 and continue (original is kept).",
            "abort": "Abort.",
        },
        interactive=interactive,
        question="UTF-8 is required for SQL import. What would you like to do?",
    )
    if action == "abort":
        raise UserAbort("Aborted: please convert the file to UTF-8 and reload.")

    utf8_path = _sibling_path(file_path, "_utf8")
    text = raw.decode(detected, errors="replace")
    with open(utf8_path, "w", encoding="utf-8", newline="") as dst:
        dst.write(text)
    stats["converted"] = True
    logger.info("Converted %s -> UTF-8 copy at %s", detected, utf8_path)
    return _ret((utf8_path, "utf-8"), stats, return_stats)


def handle_trailing_commas_preload(
    file_path: str,
    encoding: str = "utf-8",
    errors: str = "strict",
    *,
    on_trailing: str = "memory",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Find and remove trailing commas before the file is loaded into pandas.

    A trailing comma is an empty cell past the last real column. On the
    header row, that means empty names at the end of the header. On a data
    row, it means extra cells past the header's width that are all empty.

    A data row whose *last real column* is just empty, such as ``a,b,`` under
    a three-column header, is valid CSV and is left alone. Older versions of
    this function flagged and stripped those rows, which corrupted them.

    Policies for ``on_trailing``:

    * ``"memory"``: fix the rows in memory and return an ``io.StringIO``.
    * ``"save"``: write ``<name>_corrected<ext>`` and return its path.
    * ``"abort"``: raise :class:`UserAbort`.

    Returns:
        ``(data, source)``, where ``data`` is a path or an ``io.StringIO``
        and ``source`` is ``"file"`` or ``"memory"``. When ``return_stats`` is
        true, a stats dict is added with the keys ``header_flagged``,
        ``data_rows_flagged`` and ``action``.

    Raises:
        FileNotFoundError: The file does not exist.
        ValueError: The file is empty.
        UserAbort: The policy or the user chose to abort.
    """
    _require_file(file_path)

    with open(file_path, encoding=encoding, errors=errors, newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        raise ValueError(f"The file is empty: {file_path}")

    header = rows[0]
    width = len(header)
    while width > 0 and header[width - 1].strip() == "":
        width -= 1
    header_flagged = width < len(header)

    flagged_rows = [
        row_num
        for row_num, row in enumerate(rows[1:], start=2)
        if len(row) > width and all(cell.strip() == "" for cell in row[width:])
    ]

    stats = {
        "header_flagged": header_flagged,
        "data_rows_flagged": len(flagged_rows),
        "action": "none",
    }

    if not header_flagged and not flagged_rows:
        logger.info("No trailing commas detected.")
        return _ret((file_path, "file"), stats, return_stats)

    parts = []
    if header_flagged:
        parts.append("header row (this would add phantom empty columns)")
    if flagged_rows:
        parts.append(f"{len(flagged_rows)} data row(s)")
    logger.warning("Trailing commas detected on %s.", " and ".join(parts))

    action = _decide(
        on_trailing,
        {
            "save": "Remove them, save a corrected copy, and continue.",
            "memory": "Remove them in memory and continue (nothing written to disk).",
            "abort": "Abort and fix manually.",
        },
        interactive=interactive,
        question="How should trailing commas be handled?",
    )
    if action == "abort":
        raise UserAbort("Aborted: please remove trailing commas and reload.")

    flagged = set(flagged_rows)
    corrected = [header[:width]]
    for row_num, row in enumerate(rows[1:], start=2):
        corrected.append(row[:width] if row_num in flagged else row)

    stats["action"] = "saved" if action == "save" else "memory"

    if action == "save":
        out_path = _sibling_path(file_path, "_corrected")
        with open(out_path, "w", encoding=encoding, errors=errors, newline="") as f:
            csv.writer(f).writerows(corrected)
        logger.info("Trailing commas removed. Corrected copy saved to %s", out_path)
        return _ret((out_path, "file"), stats, return_stats)

    buffer = io.StringIO()
    csv.writer(buffer).writerows(corrected)
    buffer.seek(0)
    logger.info("Trailing commas removed in memory.")
    return _ret((buffer, "memory"), stats, return_stats)


def validate_column_counts_preload(
    data: CsvSource,
    encoding: str = "utf-8",
    errors: str = "strict",
    *,
    max_report: int = 20,
    return_stats: bool = False,
):
    """Check that every row has the same number of fields as the header.

    This is a structural problem that can't be fixed automatically, so the
    function reports the offending rows and raises :class:`StructuralError`.
    The full list of bad rows is attached to the exception as ``.rows``.

    Args:
        data: A file path, or the ``io.StringIO`` returned by
            :func:`handle_trailing_commas_preload`.
        encoding, errors: Only used when ``data`` is a path.
        max_report: The maximum number of bad rows to describe in the log.
        return_stats: Also return a stats dict.

    Returns:
        ``True``, or ``(True, stats)`` when ``return_stats`` is set. The stats
        dict has the keys ``expected_columns``, ``inconsistent_row_count``,
        ``too_many`` and ``too_few``.

    Raises:
        StructuralError: One or more rows have the wrong number of fields.
        ValueError: The source is empty.
    """
    f, owned = _open_source(data, encoding, errors)
    try:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"The file is empty: {data}") from None
        expected = len(header)

        bad: list[dict] = []
        for row in reader:
            if not row:  # csv yields [] for blank lines, which pandas skips
                continue
            if len(row) != expected:
                bad.append({
                    "row_number": reader.line_num,
                    "expected_columns": expected,
                    "actual_columns": len(row),
                    "row_preview": row,
                })
    finally:
        if owned:
            f.close()

    too_many = [r["row_number"] for r in bad if r["actual_columns"] > expected]
    too_few = [r["row_number"] for r in bad if r["actual_columns"] < expected]
    stats = {
        "expected_columns": expected,
        "inconsistent_row_count": len(bad),
        "too_many": too_many,
        "too_few": too_few,
    }

    if not bad:
        logger.info("Column count check passed: all rows have %d fields.", expected)
        return _ret(True, stats, return_stats)

    summary = f"{len(bad)} row(s) have the wrong number of columns (expected {expected})."
    lines = [_RULE, summary, _RULE]
    for r in bad[:max_report]:
        kind = "TOO MANY" if r["actual_columns"] > expected else "TOO FEW"
        lines.append(f"  Line {r['row_number']}: {kind} ({r['actual_columns']}) "
                     f"-> {r['row_preview']}")
    if len(bad) > max_report:
        lines.append(f"  ... and {len(bad) - max_report} more.")
    if too_many:
        lines += [
            "Too many columns usually means:",
            "  - a value containing a comma is not wrapped in quotes, or",
            "  - extra commas were typed during data entry.",
        ]
    if too_few:
        lines += [
            "Too few columns usually means:",
            "  - a line break inside an unquoted field split one row in two, or",
            "  - a column was deleted during data entry.",
        ]
    lines.append("Tip: fix these in a plain-text editor (e.g. VS Code), not Excel.")
    logger.warning("\n".join(lines))

    raise StructuralError(
        f"{len(bad)} inconsistent row(s) detected. Fix the CSV file and reload.",
        rows=bad,
    )


def load_csv(
    data: CsvSource,
    encoding: str = "utf-8",
    errors: str = "strict",
    *,
    keep_as_text: bool = True,
    return_stats: bool = False,
):
    """Load a CSV path or buffer into a DataFrame.

    By default every column is loaded as text (``keep_as_text=True``), so
    values like ``"00501"`` or ``"007"`` keep their leading zeros. Column
    types are inferred later, and more carefully, by
    :func:`infer_column_types`.

    Returns:
        A DataFrame, or ``(df, stats)`` when ``return_stats`` is set. The
        stats dict has the keys ``rows``, ``columns`` and ``column_names``.

    Raises:
        FileNotFoundError: ``data`` is a path that does not exist.
        TypeError: ``data`` is not a path or ``io.StringIO``.
        ValueError: The file is empty, has no data rows, or can't be parsed.
    """
    if isinstance(data, io.StringIO):
        data.seek(0)
    elif isinstance(data, (str, os.PathLike)):
        _require_file(data)
    else:
        raise TypeError(f"Expected a file path or io.StringIO, got {type(data).__name__}.")

    # Case variants of NULL_LIKE_VALUES. pandas matches na_values exactly,
    # and whitespace-padded variants are caught later by
    # handle_null_like_strings().
    na_values = sorted({v for base in NULL_LIKE_VALUES
                        for v in (base, base.upper(), base.capitalize())})

    try:
        df = pd.read_csv(
            data,
            encoding=encoding,
            encoding_errors=errors,
            na_values=na_values,
            keep_default_na=True,
            dtype=str if keep_as_text else None,
        )
    except pd.errors.EmptyDataError:
        raise ValueError(f"The file is empty: {data}") from None
    except pd.errors.ParserError as e:
        raise ValueError(f"Could not parse the file as CSV: {e}") from e

    if len(df.columns) == 0:
        raise ValueError(f"The file contains no columns: {data}")
    if df.empty:
        raise ValueError(f"The file has a header but no data rows: {data}")

    logger.info("Loaded %d rows x %d columns.", len(df), len(df.columns))
    stats = {"rows": len(df), "columns": len(df.columns), "column_names": df.columns.tolist()}
    return _ret(df, stats, return_stats)


def validate_column_counts_postload(df: pd.DataFrame, *, return_stats: bool = False):
    """Look for structural leftovers after loading.

    Checks for two things: auto-named ``Unnamed: N`` columns, which pandas
    creates for empty header cells, and rows that are completely empty.

    Returns:
        ``True`` if no unnamed columns were found, or ``(bool, stats)`` when
        ``return_stats`` is set. The stats dict has the keys
        ``total_columns``, ``unnamed_columns`` and ``empty_row_count``.
    """
    _require_df(df)

    unnamed = [c for c in df.columns if _UNNAMED_RE.match(str(c))]
    empty_rows = int(df.isna().all(axis=1).sum())

    if unnamed:
        logger.warning(
            "Unnamed columns detected: %s (usually empty header cells or trailing commas).",
            unnamed,
        )
    if empty_rows:
        logger.warning(
            "%d completely empty row(s) detected. drop_empty_rows() will remove them.",
            empty_rows,
        )
    if not unnamed and not empty_rows:
        logger.info("Post-load check passed. No structural issues detected.")

    stats = {"total_columns": len(df.columns), "unnamed_columns": unnamed,
             "empty_row_count": empty_rows}
    return _ret(not unnamed, stats, return_stats)


def handle_merged_headers(
    df: pd.DataFrame,
    *,
    threshold: float = 0.5,
    on_detected: str = "drop",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Find data rows that repeat the header and remove them.

    This happens when a file is exported with two header rows, or when
    several exports are concatenated together. Any row in which more than
    ``threshold`` of the cells match their own column's name (ignoring case
    and surrounding whitespace) is treated as a repeated header.

    Policies for ``on_detected``: ``"drop"``, ``"keep"`` or ``"abort"``.

    Returns:
        The DataFrame with repeated header rows removed and the index reset.
        When ``return_stats`` is set, also returns a dict with the key
        ``header_rows``, a list of the 0-based row positions found.
    """
    _require_df(df)
    stats = {"header_rows": []}

    if df.empty or len(df.columns) == 0:
        return _ret(df, stats, return_stats)

    names = pd.Index([str(c).strip().lower() for c in df.columns])
    cells = df.astype("string").apply(lambda s: s.str.strip().str.lower())
    matches = cells.eq(pd.Series(names, index=df.columns), axis=1).fillna(False)
    ratio = matches.sum(axis=1) / len(df.columns)
    hits = [int(i) for i in (ratio > threshold).to_numpy().nonzero()[0]]
    stats["header_rows"] = hits

    if not hits:
        logger.info("No repeated header rows detected.")
        return _ret(df, stats, return_stats)

    logger.warning(
        "%d row(s) look like a repeated header (0-based positions %s).",
        len(hits), hits[:10] + (["..."] if len(hits) > 10 else []),
    )
    if interactive:
        print(df.iloc[hits[:5]].to_string())

    action = _decide(
        on_detected,
        {
            "drop": "Remove these rows and continue.",
            "keep": "Keep them as data.",
            "abort": "Abort and fix the CSV manually.",
        },
        interactive=interactive,
        question="What should happen to the repeated header rows?",
    )
    if action == "abort":
        raise UserAbort("Aborted: please fix the repeated header rows manually and reload.")
    if action == "keep":
        return _ret(df, stats, return_stats)

    out = df.drop(index=df.index[hits]).reset_index(drop=True)
    logger.info("Removed %d repeated header row(s).", len(hits))
    return _ret(out, stats, return_stats)


def drop_empty_rows(df: pd.DataFrame, *, return_stats: bool = False):
    """Remove rows where every value is missing.

    Returns:
        A new DataFrame with the index reset. When ``return_stats`` is set,
        also returns a dict with the key ``dropped``.
    """
    _require_df(df)
    mask = df.isna().all(axis=1)
    dropped = int(mask.sum())
    out = df.loc[~mask].reset_index(drop=True) if dropped else df.copy()
    if dropped:
        logger.info("Dropped %d completely empty row(s).", dropped)
    return _ret(out, {"dropped": dropped}, return_stats)


# ============================================================
# Phase 2: Header cleaning
# ============================================================


def handle_empty_column_names(
    df: pd.DataFrame,
    *,
    on_empty: str = "rename",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Strip whitespace from column names and give empty ones a name.

    pandas never produces an empty column name. It replaces empty header
    cells with ``Unnamed: N``. Those names, and names that are blank after
    stripping, are treated as empty here. With ``on_empty="rename"`` they
    become ``col_<index>``. With ``"abort"``, :class:`UserAbort` is raised.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        with the keys ``stripped`` (old -> new) and ``renamed`` (index -> new).
    """
    _require_df(df)
    out = df.copy()

    original = [str(c) for c in out.columns]
    stripped_names = [c.strip() for c in original]
    stripped = {o: s for o, s in zip(original, stripped_names) if o != s}
    if stripped:
        logger.info("Stripped whitespace from %d column name(s): %s", len(stripped), stripped)

    empty_idx = [i for i, c in enumerate(stripped_names) if c == "" or _UNNAMED_RE.match(c)]
    stats = {"stripped": stripped, "renamed": {}}

    if not empty_idx:
        out.columns = stripped_names
        return _ret(out, stats, return_stats)

    taken = set(stripped_names)
    proposed = {}
    for i in empty_idx:
        name = f"col_{i}"
        while name in taken:
            name += "_"
        taken.add(name)
        proposed[i] = name

    logger.warning(
        "Empty column name(s) at position(s) %s. Proposed names: %s",
        empty_idx, list(proposed.values()),
    )
    action = _decide(
        on_empty,
        {"rename": "Apply the proposed names and continue.", "abort": "Abort and fix manually."},
        interactive=interactive,
        question="How should empty column names be handled?",
    )
    if action == "abort":
        raise UserAbort("Aborted: please name every column and reload.")

    out.columns = [proposed.get(i, c) for i, c in enumerate(stripped_names)]
    stats["renamed"] = proposed
    return _ret(out, stats, return_stats)


def _sanitize_name(name: str, *, ascii_only: bool) -> str:
    if ascii_only:
        name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = _NONWORD_RE.sub("_", name)
    return _MULTI_US_RE.sub("_", name).strip("_")


def handle_special_characters_in_headers(
    df: pd.DataFrame,
    *,
    ascii_only: bool = True,
    on_special: str = "replace",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Replace characters in column names that aren't safe in SQL.

    Each run of characters that are not letters, digits or underscores
    becomes a single underscore, and underscores at the ends are trimmed.
    For example, ``"First Name"`` becomes ``"First_Name"``. Older versions
    deleted the characters instead, which turned that name into
    ``"FirstName"``. With ``ascii_only=True``, accented letters are
    transliterated first, so ``"Café"`` becomes ``"Cafe"``.

    Any duplicate names this creates are left in place for
    :func:`handle_duplicate_column_names` to resolve.

    Policies for ``on_special``: ``"replace"`` or ``"abort"``.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        with the key ``renamed`` (old -> new).
    """
    _require_df(df)
    out = df.copy()
    old = [str(c) for c in out.columns]
    new = [_sanitize_name(c, ascii_only=ascii_only) or c for c in old]
    renamed = {o: n for o, n in zip(old, new) if o != n}
    stats = {"renamed": renamed}

    if not renamed:
        logger.info("No special characters in column names.")
        return _ret(out, stats, return_stats)

    logger.warning(
        "Special characters in column names:\n%s",
        "\n".join(f"  - {o!r} -> {n!r}" for o, n in renamed.items()),
    )
    action = _decide(
        on_special,
        {"replace": "Apply these renames and continue.", "abort": "Abort and fix manually."},
        interactive=interactive,
        question="How should special characters be handled?",
    )
    if action == "abort":
        raise UserAbort("Aborted: please rename the columns manually and reload.")

    if len(set(new)) != len(new):
        logger.warning("Renaming created duplicate column names. These will be resolved next.")
    out.columns = new
    return _ret(out, stats, return_stats)


def to_snake_case(name) -> str:
    """Convert a single name to a SQL-friendly snake_case identifier.

    >>> to_snake_case("customerID")
    'customer_id'
    >>> to_snake_case("HTTPResponseCode")
    'http_response_code'
    >>> to_snake_case("2024 Sales ($)")
    'col_2024_sales'
    """
    name = str(name).strip()
    name = _ACRONYM_RE.sub(r"\1_\2", name)
    name = _CAMEL_RE.sub(r"\1_\2", name)
    name = _NONWORD_RE.sub("_", name).lower()
    name = _MULTI_US_RE.sub("_", name).strip("_")
    if not name:
        return "col"
    if name[0].isdigit():  # unquoted SQL identifiers can't start with a digit
        name = f"col_{name}"
    return name


def convert_headers_to_snake_case(df: pd.DataFrame, *, return_stats: bool = False):
    """Convert every column name to snake_case.

    This step never asks a question: snake_case is always safe for both SQL
    and pandas.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        with the key ``renamed`` (old -> new).
    """
    _require_df(df)
    out = df.copy()
    new = [to_snake_case(c) for c in out.columns]
    renamed = {str(o): n for o, n in zip(out.columns, new) if str(o) != n}
    if renamed:
        logger.info("Converted %d column name(s) to snake_case.", len(renamed))
    out.columns = new
    return _ret(out, {"renamed": renamed}, return_stats)


def handle_duplicate_column_names(
    df: pd.DataFrame,
    *,
    on_duplicate: str = "suffix",
    interactive: bool = False,
    return_stats: bool = False,
):
    """Resolve duplicate column names.

    Header cleaning can create duplicates. For example, ``"First Name"`` and
    ``"First-Name"`` both become ``first_name``.

    Policies for ``on_duplicate`` (the first occurrence is always kept as is):

    * ``"suffix"``: rename later occurrences to ``name_2``, ``name_3``, and
      so on.
    * ``"drop"``: drop later occurrences.
    * ``"abort"``: raise :class:`UserAbort`.

    With ``interactive=True``, each later occurrence can be renamed by hand
    or dropped.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        with the keys ``renamed`` (index -> new name) and ``dropped`` (a list
        of indices).
    """
    _require_df(df)
    out = df.copy()
    names = [str(c) for c in out.columns]
    stats = {"renamed": {}, "dropped": []}

    dupes = sorted({n for n in names if names.count(n) > 1})
    if not dupes:
        logger.info("No duplicate column names.")
        out.columns = names
        return _ret(out, stats, return_stats)

    logger.warning("Duplicate column names detected: %s", dupes)
    action = _decide(
        on_duplicate,
        {
            "suffix": "Rename later occurrences to name_2, name_3, ...",
            "drop": "Drop later occurrences.",
            "abort": "Abort and fix manually.",
        },
        interactive=False,  # validates the policy; interactive prompts are per column below
        question="",
    )
    if action == "abort" and not interactive:
        raise UserAbort("Aborted: please resolve the duplicate column names and reload.")

    taken = set(names)
    drop: set[int] = set()

    for dup in dupes:
        positions = [i for i, n in enumerate(names) if n == dup]
        for k, idx in enumerate(positions[1:], start=2):
            if interactive:
                choice = _decide(
                    action,
                    {"suffix": f"Rename to an automatic suffix ({dup}_{k}).",
                     "rename": "Enter a new name.",
                     "drop": "Drop this column.",
                     "abort": "Abort."},
                    interactive=True,
                    question=f"Column {dup!r} (occurrence {k}, position {idx}):",
                )
            else:
                choice = action

            if choice == "abort":
                raise UserAbort("Aborted: please resolve the duplicate column names and reload.")
            if choice == "drop":
                drop.add(idx)
                continue
            if choice == "rename":
                while True:
                    new = input("    New name: ").strip()
                    if new and new not in taken:
                        break
                    print("    Name must be non-empty and not already in use.")
            else:
                new, n = f"{dup}_{k}", k
                while new in taken:
                    n += 1
                    new = f"{dup}_{n}"
            names[idx] = new
            taken.add(new)
            stats["renamed"][idx] = new

    keep = [i for i in range(len(names)) if i not in drop]
    out = out.iloc[:, keep]
    out.columns = [names[i] for i in keep]
    stats["dropped"] = sorted(drop)
    logger.info("Duplicate column names resolved.")
    return _ret(out, stats, return_stats)


# ============================================================
# Phase 3: Value cleaning
# ============================================================


def _text_columns(df: pd.DataFrame) -> list:
    return df.select_dtypes(include=["object", "string"]).columns.tolist()


def strip_string_values(df: pd.DataFrame, *, return_stats: bool = False):
    """Trim leading and trailing whitespace from every text value.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        mapping each column name to the number of values that changed.
    """
    _require_df(df)
    out = df.copy()
    changed: dict[str, int] = {}
    for col in _text_columns(out):
        s = out[col]
        is_str = s.map(lambda v: isinstance(v, str))
        if not is_str.any():
            continue
        stripped = s.where(~is_str, s[is_str].str.strip())
        n = int((stripped[is_str] != s[is_str]).sum())
        if n:
            out[col] = stripped
            changed[str(col)] = n
    if changed:
        logger.info("Stripped surrounding whitespace in %d column(s).", len(changed))
    return _ret(out, changed, return_stats)


def handle_null_like_strings(df: pd.DataFrame, *, return_stats: bool = False):
    """Replace null-like strings such as ``" N/A "`` or ``"--"`` with missing values.

    This catches the values that slip past ``pd.read_csv``, usually because
    of surrounding whitespace or unusual capitalisation. Only text columns
    are checked. See :data:`NULL_LIKE_VALUES` for the list of strings.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        mapping each column name to the number of values replaced.
    """
    _require_df(df)
    out = df.copy()
    replacements: dict[str, int] = {}

    for col in _text_columns(out):
        s = out[col]
        normalized = s.astype("string").str.strip().str.lower()
        mask = normalized.isin(NULL_LIKE_VALUES).fillna(False).to_numpy(dtype=bool)
        if mask.any():
            out[col] = s.mask(mask)  # positional mask: safe with duplicate index labels
            replacements[str(col)] = int(mask.sum())

    if replacements:
        logger.info(
            "Replaced null-like strings:\n%s",
            "\n".join(f"  - {c}: {n}" for c, n in replacements.items()),
        )
    else:
        logger.info("No null-like strings detected.")
    return _ret(out, replacements, return_stats)


def infer_column_types(
    df: pd.DataFrame,
    *,
    numeric_bools: bool = False,
    preserve_leading_zeros: bool = True,
    return_stats: bool = False,
):
    """Cast text columns to boolean, integer or float where it is safe.

    A column is only converted if *every* non-missing value converts. Rules:

    * **Boolean**: every value is true/false, yes/no, t/f or y/n (any
      capitalisation). Columns of just ``0``/``1`` stay numeric unless
      ``numeric_bools=True``, because a quantity column that happens to
      hold only 0s and 1s is not a flag.
    * **Integer**: every value is numeric and whole. The column becomes
      pandas' nullable ``Int64``, so missing values don't force a float.
    * **Float**: every value is numeric.
    * **Left as text**: values with leading zeros such as ZIP codes or IDs
      (when ``preserve_leading_zeros``), and ``inf``.

    Dates are not handled yet. They are planned for a later pass.

    Returns:
        A new DataFrame. When ``return_stats`` is set, also returns a dict
        mapping each converted column to its ``(old_dtype, new_dtype)``.
    """
    _require_df(df)
    out = df.copy()
    conversions: dict[str, tuple[str, str]] = {}

    bool_map = {**BOOL_MAP, **(_NUMERIC_BOOL_MAP if numeric_bools else {})}

    for col in _text_columns(out):
        s = out[col]
        present = s.notna()
        if not present.any():
            continue
        old_dtype = str(s.dtype)
        values = s[present].astype(str).str.strip()
        lowered = values.str.lower()

        if lowered.isin(bool_map).all():
            out[col] = s.map(
                lambda v: bool_map[str(v).strip().lower()] if pd.notna(v) else pd.NA
            ).astype("boolean")
            conversions[str(col)] = (old_dtype, "boolean")
            continue

        if preserve_leading_zeros and values.str.match(_LEADING_ZERO_RE).any():
            continue

        nums = pd.to_numeric(values, errors="coerce")
        if nums.isna().any():
            continue
        if not pd.Series(nums).map(lambda x: abs(x) != float("inf")).all():
            continue

        if (nums % 1 == 0).all():
            converted = pd.Series(pd.NA, index=s.index, dtype="Int64")
            converted[present] = nums.astype("int64")
        else:
            converted = pd.Series(float("nan"), index=s.index, dtype="float64")
            converted[present] = nums.astype("float64")
        out[col] = converted
        conversions[str(col)] = (old_dtype, str(converted.dtype))

    if conversions:
        logger.info(
            "Inferred column types:\n%s",
            "\n".join(f"  - {c}: {o} -> {n}" for c, (o, n) in conversions.items()),
        )
    else:
        logger.info("No column type conversions performed.")
    return _ret(out, conversions, return_stats)


# ============================================================
# Pipeline
# ============================================================


def clean_csv(
    file_path: str,
    *,
    interactive: bool = False,
    on_non_utf8: str = "convert",
    on_trailing: str = "memory",
    on_merged_header: str = "drop",
    on_empty_name: str = "rename",
    on_duplicate: str = "suffix",
    numeric_bools: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """Run the whole pipeline on a single CSV file.

    Every ``on_*`` argument is passed through to the step it belongs to.
    See the individual functions for the values each one accepts.

    Returns:
        ``(df, report)``. ``report`` maps each step name to that step's
        stats dict.
    """
    report: dict[str, dict] = {}
    kw = {"interactive": interactive, "return_stats": True}

    path, enc, report["encoding"] = handle_encoding_issues(
        file_path, on_non_utf8=on_non_utf8, **kw)
    data, _, report["trailing_commas"] = handle_trailing_commas_preload(
        path, enc, on_trailing=on_trailing, **kw)
    _, report["column_counts"] = validate_column_counts_preload(data, enc, return_stats=True)

    df, report["load"] = load_csv(data, enc, return_stats=True)
    _, report["postload"] = validate_column_counts_postload(df, return_stats=True)
    df, report["merged_headers"] = handle_merged_headers(df, on_detected=on_merged_header, **kw)
    df, report["empty_rows"] = drop_empty_rows(df, return_stats=True)

    df, report["empty_names"] = handle_empty_column_names(df, on_empty=on_empty_name, **kw)
    df, report["special_chars"] = handle_special_characters_in_headers(df, **kw)
    df, report["snake_case"] = convert_headers_to_snake_case(df, return_stats=True)
    df, report["duplicates"] = handle_duplicate_column_names(df, on_duplicate=on_duplicate, **kw)

    df, report["whitespace"] = strip_string_values(df, return_stats=True)
    df, report["null_like"] = handle_null_like_strings(df, return_stats=True)
    df, report["types"] = infer_column_types(df, numeric_bools=numeric_bools, return_stats=True)

    return df, report


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cleaner.py",
        description="Clean a CSV file and write UTF-8 output ready for SQL import.",
    )
    p.add_argument("input", help="Path to the CSV file to clean.")
    p.add_argument("-o", "--output",
                   help="Where to write the cleaned CSV (default: <input>_clean.csv).")
    p.add_argument("-i", "--interactive", action="store_true",
                   help="Prompt before each fix instead of applying the default policies.")
    p.add_argument("-v", "--verbose", action="store_true", help="Show progress messages.")
    p.add_argument("--on-non-utf8", choices=["convert", "abort"], default="convert")
    p.add_argument("--on-trailing", choices=["memory", "save", "abort"], default="memory")
    p.add_argument("--on-merged-header", choices=["drop", "keep", "abort"], default="drop")
    p.add_argument("--on-empty-name", choices=["rename", "abort"], default="rename")
    p.add_argument("--on-duplicate", choices=["suffix", "drop", "abort"], default="suffix")
    p.add_argument("--numeric-bools", action="store_true",
                   help="Treat columns of only 0/1 as boolean.")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )
    try:
        df, _ = clean_csv(
            args.input,
            interactive=args.interactive,
            on_non_utf8=args.on_non_utf8,
            on_trailing=args.on_trailing,
            on_merged_header=args.on_merged_header,
            on_empty_name=args.on_empty_name,
            on_duplicate=args.on_duplicate,
            numeric_bools=args.numeric_bools,
        )
    except (CleaningError, FileNotFoundError, ValueError) as e:
        logger.error("%s", e)
        return 1

    out_path = args.output or _sibling_path(args.input, "_clean")
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows x {len(df.columns)} columns to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
