# py-cleaner

![tests](https://github.com/stapleybc/py-cleaner/actions/workflows/tests.yml/badge.svg)

A small, well-tested Python module that turns messy CSV exports into clean,
UTF-8, SQL-ready data.

It fixes the problems that break database imports: the wrong encoding,
trailing commas, repeated header rows, column names like `Amount ($)`,
`"N/A"` strings posing as values, and ZIP codes that lose their leading zeros.

## Before and after

**Input** (`examples/messy.csv`):

```csv
Customer ID,First Name,First-Name,Zip Code,Active,Amount ($),
Customer ID,First Name,First-Name,Zip Code,Active,Amount ($),
1, Ann ,A,00501,yes,10.50,
2,Bob,B,02134,NO, N/A ,
,,,,,,
3,Cy,C,90210,Yes,7,
```

**Output:**

```csv
customer_id,first_name,first_name_2,zip_code,active,amount
1,Ann,A,00501,True,10.5
2,Bob,B,02134,False,
3,Cy,C,90210,True,7.0
```

## Installation

```bash
git clone https://github.com/stapleybc/py-cleaner.git
cd py-cleaner
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Command line

```bash
python cleaner.py examples/messy.csv -o clean.csv      # apply the default fixes
python cleaner.py examples/messy.csv --interactive     # ask before each fix
python cleaner.py examples/messy.csv -v                # show every step
```

Every fix has a flag that controls it. For example:

| Flag | Choices (default first) |
|---|---|
| `--on-non-utf8` | `convert`, `abort` |
| `--on-trailing` | `memory`, `save`, `abort` |
| `--on-merged-header` | `drop`, `keep`, `abort` |
| `--on-empty-name` | `rename`, `abort` |
| `--on-duplicate` | `suffix`, `drop`, `abort` |
| `--numeric-bools` | treat columns of only `0`/`1` as booleans |

The command exits with code `1` when a file can't be cleaned safely, for
example when rows have different numbers of columns. That makes it easy to
use in scripts.

### Python

```python
from cleaner import clean_csv

df, report = clean_csv("data.csv")
print(report["types"])   # {'customer_id': ('str', 'Int64'), ...}
```

Each step is also a standalone function, so you can build your own pipeline:

```python
import cleaner as c

df = c.load_csv("data.csv")
df = c.convert_headers_to_snake_case(df)
df = c.handle_null_like_strings(df)
df = c.infer_column_types(df)
```

## What it does

| Phase | Step | What it fixes |
|---|---|---|
| **1. Ingestion** | `handle_encoding_issues` | Converts files that aren't UTF-8 (such as Windows-1252) to a UTF-8 copy. |
| | `handle_trailing_commas_preload` | Removes phantom empty columns caused by trailing commas. |
| | `validate_column_counts_preload` | Reports rows with too many or too few fields, with line numbers. |
| | `load_csv` | Loads every column as text, so leading zeros are kept. |
| | `handle_merged_headers` | Removes repeated header rows, such as those in concatenated exports. |
| | `drop_empty_rows` | Removes rows that are completely blank. |
| **2. Headers** | `handle_empty_column_names` | Names blank or `Unnamed: N` columns. |
| | `handle_special_characters_in_headers` | `Amount ($)` → `Amount`, `Café` → `Cafe` |
| | `convert_headers_to_snake_case` | `customerID` → `customer_id`, `2024 Sales` → `col_2024_sales` |
| | `handle_duplicate_column_names` | `first_name`, `first_name` → `first_name`, `first_name_2` |
| **3. Values** | `strip_string_values` | Trims whitespace around values. |
| | `handle_null_like_strings` | Turns `" N/A "`, `"--"`, `"null"` and similar into real missing values. |
| | `infer_column_types` | Converts columns to boolean, nullable integer or float, but only when every value converts. |

### Design choices

- **Never modifies your original file.** Any converted or corrected copy is
  written next to it with a suffix.
- **Careful type inference.** A column is only converted when every value
  converts. Values with leading zeros, like `00501`, stay as text. A column
  holding only 0s and 1s stays numeric unless you ask for booleans.
- **Runs unattended or interactively.** Every decision is a policy argument,
  so the same code works in scripts, tests and an interactive session.
- **Uses standard logging.** Warnings are always shown. Use `-v` or
  `logging.basicConfig(level=logging.INFO)` to see the details.

## Development

```bash
pip install -r requirements-dev.txt
pytest          # unit tests + doctests
ruff check .    # lint
```

Tests run on Python 3.10–3.13 through GitHub Actions on every push.

## Roadmap

- Date and datetime detection
- Configurable validation rules (YAML)
- Direct SQL output (`CREATE TABLE` + bulk load)

## License

MIT
