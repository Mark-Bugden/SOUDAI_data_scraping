import json
import threading
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from scraping.infosoud.utils.checkpointing import (
    deduplicate_checkpoint,
    validate_checkpoint,
)
from scraping.infosoud.utils.court_data import court_lookup
from scraping.infosoud.utils.filtering import (
    filter_out_bad_case_numbers,
    filter_out_bad_court_names,
)
from scraping.infosoud.utils.io import load_all_decisions, load_done_urls
from scraping.infosoud.utils.parsing import add_parsed_jednaciCislo, parse_jednaciCislo
from scraping.infosoud.utils.threading import StopFlag, listen_for_quit
from scraping.infosoud.utils.timeline import (
    extract_case_timeline,
    process_chunk,
)
from scraping.infosoud.utils.urls import add_infosoud_urls, create_infosoud_URL

# ==========================================
# io tests
# ==========================================


def test_load_all_decisions(tmp_path):
    # Create nested structure like data/raw/2024/01/02/
    nested_dir = tmp_path / "2024" / "01" / "02"
    nested_dir.mkdir(parents=True)

    # Write a mock page0.json file
    sample_data = {"items": [{"id": "abc", "soud": "Okresní soud v Brně"}]}
    file_path = nested_dir / "page0.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(sample_data, f)

    # Call the function
    df = load_all_decisions(tmp_path)

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["id"] == "abc"
    assert df.iloc[0]["soud"] == "Okresní soud v Brně"


def test_load_all_decisions_with_corrupt_file(tmp_path, capsys):
    # Create nested structure
    nested_dir = tmp_path / "2024" / "01" / "02"
    nested_dir.mkdir(parents=True)

    # Valid JSON file
    valid_data = {"items": [{"id": "abc", "soud": "Okresní soud v Brně"}]}
    with open(nested_dir / "page0.json", "w", encoding="utf-8") as f:
        json.dump(valid_data, f)

    # Corrupted JSON file
    with open(nested_dir / "page1.json", "w", encoding="utf-8") as f:
        f.write("{ not valid json ")

    # Call the function
    df = load_all_decisions(tmp_path)

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["id"] == "abc"

    # Check that an error was printed for the corrupt file
    captured = capsys.readouterr()
    assert "Failed to load" in captured.out
    assert "page1.json" in captured.out


def test_load_done_urls(tmp_path: Path):
    # Create a mock checkpoint CSV file
    csv_path = tmp_path / "checkpoint.csv"

    df = pd.DataFrame(
        {
            "infosoud_url": [
                "https://infosoud.justice.cz/InfoSoud/public/search.do?dummy=1",
                "https://infosoud.justice.cz/InfoSoud/public/search.do?dummy=2",
            ]
        }
    )
    df.to_csv(csv_path, index=False)

    result = load_done_urls(csv_path)
    assert isinstance(result, set)
    assert len(result) == 2
    assert all(isinstance(url, str) for url in result)


def test_load_done_urls_missing_column(tmp_path: Path):
    # Create a file missing the 'infosoud_url' column
    csv_path = tmp_path / "checkpoint.csv"

    df = pd.DataFrame({"some_other_column": ["a", "b", "c"]})
    df.to_csv(csv_path, index=False)

    result = load_done_urls(csv_path)
    assert result == set()


def test_load_done_urls_no_file(tmp_path: Path):
    # Path doesn't exist
    fake_path = tmp_path / "nonexistent.csv"
    result = load_done_urls(fake_path)
    assert result == set()


# ==========================================
# Jednaci cislo parsing tests
# ==========================================


def test_parse_valid_with_dash():
    result = parse_jednaciCislo("12 C 123/2020-15")
    assert result == [12, "C", 123, 2020, 15]


def test_parse_valid_without_dash():
    result = parse_jednaciCislo("3 T 456/2021")
    assert result == [3, "T", 456, 2021, None]


def test_parse_invalid_format():
    assert parse_jednaciCislo("bad input") is None
    assert parse_jednaciCislo("12C123/2020") is None
    assert parse_jednaciCislo("12 C") is None
    assert parse_jednaciCislo("12C123/2020") is None
    assert parse_jednaciCislo("12 C") is None


def test_add_parsed_jednaciCislo_column_added_correctly():
    df = pd.DataFrame(
        {
            "jednaciCislo": [
                "12 C 123/2020-15",  # valid
                "3 T 456/2021",  # valid without dash
                "invalid value",  # invalid
            ]
        }
    )
    result_df = add_parsed_jednaciCislo(df)

    assert "parsed_jednaciCislo" in result_df.columns
    assert result_df.shape[0] == 3
    assert result_df["parsed_jednaciCislo"].tolist() == [
        [12, "C", 123, 2020, 15],
        [3, "T", 456, 2021, None],
        None,
    ]


# ==========================================
# Filtering tests
# ==========================================


def test_filter_out_bad_court_names():
    known_court = next(iter(court_lookup.keys()))
    unknown_court = "Neznámý soud"

    df = pd.DataFrame({"soud": [known_court, unknown_court, None]})

    result = filter_out_bad_court_names(df)

    assert len(result) == 1
    assert result.iloc[0]["soud"] == known_court


def test_filter_out_bad_case_numbers():
    df = pd.DataFrame(
        {
            "jednaciCislo": [
                "12 C 123/2020-15",  # valid
                "3 T 456/2021",  # valid (no dash)
                "not a case number",  # invalid
                "99 / / ",  # invalid
                None,  # invalid
            ]
        }
    )

    result = filter_out_bad_case_numbers(df)

    assert len(result) == 2
    assert result["jednaciCislo"].tolist() == [
        "12 C 123/2020-15",
        "3 T 456/2021",
    ]


# ==========================================
# URL tests
# ==========================================


def test_create_infosoud_URL_valid_input():
    # Use a valid court from the loaded court_lookup
    court_name = next(iter(court_lookup.keys()))
    parsed_case = [12, "C", 456, 2022, 5]

    url = create_infosoud_URL(court_name, parsed_case)

    assert url is not None
    assert "typSoudu" in url
    assert "cisloSenatu=12" in url
    assert "druhVec=C" in url
    assert "bcVec=456" in url
    assert "rocnik=2022" in url


def test_create_infosoud_URL_invalid_inputs():
    # Find a valid court name from the loaded court_lookup
    valid_court_name = next(iter(court_lookup.keys()))

    # Test unknown court name
    url1 = create_infosoud_URL("Invalid Court", [12, "C", 456, 2022, 5])
    assert url1 is None

    # Test None as parsed case but with valid court
    url2 = create_infosoud_URL(valid_court_name, None)
    assert url2 is None

    # Missing fields in parsed case (not enough parts)
    url3 = create_infosoud_URL(valid_court_name, [12, "C", 456])
    assert url3 is None

    # Missing fields in parsed case (not enough parts)
    url4 = create_infosoud_URL(valid_court_name, [12, "C", 456, None])
    assert url4 is None


def test_add_infosoud_urls_mixed_rows():
    # Use a valid court name from court_lookup
    valid_court = next(iter(court_lookup.keys()))

    df = pd.DataFrame(
        [
            {
                "soud": valid_court,
                "parsed_jednaciCislo": [12, "C", 456, 2022, 5],
            },
            {
                "soud": "Invalid Court",
                "parsed_jednaciCislo": [12, "C", 456, 2022, 5],
            },
            {
                "soud": valid_court,
                "parsed_jednaciCislo": None,
            },
        ]
    )

    result = add_infosoud_urls(df)

    assert "infosoud_url" in result.columns
    assert result["infosoud_url"].iloc[0] is not None
    assert result["infosoud_url"].iloc[1] is None
    assert result["infosoud_url"].iloc[2] is None


# ==========================================
# Checkpointing tests
# ==========================================


def test_deduplicate_checkpoint(tmp_path: Path):
    # Setup: write a checkpoint CSV with duplicates
    csv_path = tmp_path / "checkpoint.csv"
    df = pd.DataFrame(
        {
            "infosoud_url": [
                "url_1",
                "url_2",
                "url_1",  # duplicate
                "url_3",
                "url_2",  # duplicate
            ],
            "other_col": ["a", "b", "a", "c", "b"],
        }
    )
    df.to_csv(csv_path, index=False)

    # Run deduplication
    deduplicate_checkpoint(csv_path)

    # Reload the file and check
    df_deduped = pd.read_csv(csv_path, dtype=str)
    assert df_deduped.shape[0] == 3
    assert set(df_deduped["infosoud_url"]) == {"url_1", "url_2", "url_3"}

    # Run again to confirm it's idempotent
    deduplicate_checkpoint(csv_path)
    df_deduped_2 = pd.read_csv(csv_path, dtype=str)
    assert df_deduped.equals(df_deduped_2)


def test_validate_checkpoint_success(tmp_path: Path):
    # Simulate preprocessed data
    df_preprocessed = pd.DataFrame({"infosoud_url": ["url_1", "url_2", "url_3"]})

    # Valid checkpoint: subset of preprocessed
    checkpoint_path = tmp_path / "checkpoint.csv"
    df_checkpoint = pd.DataFrame({"infosoud_url": ["url_1", "url_2"]})
    df_checkpoint.to_csv(checkpoint_path, index=False)

    # Should return True
    assert validate_checkpoint(df_preprocessed, checkpoint_path) is True


def test_validate_checkpoint_missing_urls(tmp_path: Path):
    df_preprocessed = pd.DataFrame({"infosoud_url": ["url_1", "url_2"]})
    checkpoint_path = tmp_path / "checkpoint.csv"
    df_checkpoint = pd.DataFrame({"infosoud_url": ["url_1", "url_3"]})
    df_checkpoint.to_csv(checkpoint_path, index=False)

    with pytest.raises(ValueError, match="not present in the source data"):
        validate_checkpoint(df_preprocessed, checkpoint_path)


def test_validate_checkpoint_duplicate_urls(tmp_path: Path):
    df_preprocessed = pd.DataFrame({"infosoud_url": ["url_1", "url_2", "url_3"]})
    checkpoint_path = tmp_path / "checkpoint.csv"
    df_checkpoint = pd.DataFrame({"infosoud_url": ["url_1", "url_1"]})
    df_checkpoint.to_csv(checkpoint_path, index=False)

    with pytest.raises(ValueError, match="duplicate infosoud_url entries"):
        validate_checkpoint(df_preprocessed, checkpoint_path)


def test_validate_checkpoint_no_file(tmp_path: Path):
    df_preprocessed = pd.DataFrame({"infosoud_url": ["url_1", "url_2"]})
    nonexistent_path = tmp_path / "missing.csv"

    # Should print a message and return True
    assert validate_checkpoint(df_preprocessed, nonexistent_path) is True

    # Should print a message and return True
    assert validate_checkpoint(df_preprocessed, nonexistent_path) is True


# ==========================================
# Timeline tests
# ==========================================


def mock_response_with_html(html: str) -> Mock:
    mock = Mock()
    mock.status_code = 200
    mock.text = html
    return mock


def test_extract_case_timeline_success():
    html = """
    <html>
    <body>
        <h3>Průběh řízení</h3>
        <table>
            <tr><th>Event</th><th>Date</th></tr>
            <tr><td><a>Zahájení řízení</a></td><td>2022-01-01</td></tr>
            <tr><td><a>Vydání rozhodnutí</a></td><td>2022-02-01</td></tr>
        </table>
    </body>
    </html>
    """
    with patch(
        "scraping.infosoud.utils.timeline.requests.get",
        return_value=mock_response_with_html(html),
    ):
        result = extract_case_timeline("http://fake-url")
        assert result == {
            "Zahájení řízení": "2022-01-01",
            "Vydání rozhodnutí": "2022-02-01",
        }


def test_extract_case_timeline_missing_heading():
    html = "<html><body><p>No timeline here</p></body></html>"
    with patch(
        "scraping.infosoud.utils.timeline.requests.get",
        return_value=mock_response_with_html(html),
    ):
        result = extract_case_timeline("http://fake-url")
        assert result == {}


def test_extract_case_timeline_malformed_table():
    html = """
    <html><body>
        <h3>Průběh řízení</h3>
        <p>No table follows</p>
    </body></html>
    """
    with patch(
        "scraping.infosoud.utils.timeline.requests.get",
        return_value=mock_response_with_html(html),
    ):
        result = extract_case_timeline("http://fake-url")
        assert result == {}


def test_process_chunk_adds_timeline_fields(mocker):
    # Dummy input DataFrame
    df_chunk = pd.DataFrame(
        [
            {
                "id": "abc",
                "infosoud_url": "http://example.com/case1",
                "some_column": "value1",
            },
            {
                "id": "def",
                "infosoud_url": "http://example.com/case2",
                "some_column": "value2",
            },
        ]
    )

    # Dummy return value for mocked extract_case_timeline
    fake_timeline = {
        "Zahájení řízení": "2020-01-01",
        "Nařízení jednání": None,
        "Vydání rozhodnutí": "2020-06-01",
        "Vyřízení věci": None,
        "Datum pravomocného ukončení věci": "2020-07-01",
    }

    # Mock the extract_case_timeline function
    mocker.patch(
        "scraping.infosoud.utils.timeline.extract_case_timeline",
        return_value=fake_timeline,
    )

    # Run the function
    df_result = process_chunk(df_chunk)

    # Assertions
    assert len(df_result) == 2
    for col in fake_timeline:
        assert f"timeline_{col}" in df_result.columns
    assert df_result.loc[0, "timeline_Zahájení řízení"] == "2020-01-01"
    assert df_result.loc[1, "timeline_Vydání rozhodnutí"] == "2020-06-01"


def test_process_chunk_handles_extract_errors(mocker):
    # Dummy input DataFrame
    df_chunk = pd.DataFrame(
        [
            {
                "id": "abc",
                "infosoud_url": "http://example.com/ok",
                "some_column": "value1",
            },
            {
                "id": "def",
                "infosoud_url": "http://example.com/broken",
                "some_column": "value2",
            },
        ]
    )

    # Mock extract_case_timeline
    def mock_extract_case_timeline(url):
        if "broken" in url:
            raise ValueError("Simulated failure")
        return {
            "Zahájení řízení": "2020-01-01",
            "Nařízení jednání": None,
            "Vydání rozhodnutí": "2020-06-01",
            "Vyřízení věci": None,
            "Datum pravomocného ukončení věci": "2020-07-01",
        }

    mocker.patch(
        "scraping.infosoud.utils.timeline.extract_case_timeline",
        side_effect=mock_extract_case_timeline,
    )

    # Run
    df_result = process_chunk(df_chunk)

    # Assertions
    assert df_result.loc[0, "timeline_Zahájení řízení"] == "2020-01-01"
    assert df_result.loc[1, "timeline_Zahájení řízení"] is None


# ==========================================
# Threading tests
# ==========================================


def test_listen_for_quit_sets_flag():
    stop_flag = StopFlag()

    with patch("builtins.input", return_value="q"), patch("builtins.print"):
        thread = threading.Thread(
            target=listen_for_quit, args=(stop_flag,), daemon=True
        )
        thread.start()
        thread.join(timeout=2)

    assert stop_flag.is_requested() is True


def test_listen_for_quit_ignores_non_q():
    stop_flag = StopFlag()

    with patch("builtins.input", return_value="nope"), patch("builtins.print"):
        thread = threading.Thread(
            target=listen_for_quit, args=(stop_flag,), daemon=True
        )
        thread.start()
        thread.join(timeout=2)

    assert stop_flag.is_requested() is False
