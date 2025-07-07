import json
from datetime import date

from scraping.rozhodnuti import utils


def test_make_output_path():
    d = date(2023, 12, 25)
    path = utils.make_output_path(d, page=2)
    assert path == "data/raw/2023/12/25/page2.json"


def test_save_json(tmp_path):
    sample = {"foo": "bar"}
    dest = tmp_path / "test.json"
    utils.save_json(sample, str(dest))

    with open(dest, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == sample


def test_get_available_years(mocker):
    mocker.patch(
        "scraping.rozhodnuti.utils.fetch_json",
        return_value=[
            {
                "rok": 2020,
                "pocet": 797,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020",
            },
            {
                "rok": 2023,
                "pocet": 85467,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2023",
            },
        ],
    )
    years = utils.get_available_years()
    assert years == [2020, 2023]


def test_get_months_for_year(mocker):
    mocker.patch(
        "scraping.rozhodnuti.utils.fetch_json",
        return_value=[
            {
                "rok": 2020,
                "mesic": 10,
                "pocet": 26,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/10",
            },
            {
                "rok": 2020,
                "mesic": 11,
                "pocet": 294,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/11",
            },
            {
                "rok": 2020,
                "mesic": 12,
                "pocet": 477,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/12",
            },
        ],
    )
    months = utils.get_months_for_year(2020)
    assert months == [10, 11, 12]


def test_get_days_for_month(mocker):
    mocker.patch(
        "scraping.rozhodnuti.utils.fetch_json",
        return_value=[
            {
                "datum": "2021-10-20",
                "pocet": 4,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/10/20",
            },
            {
                "datum": "2021-10-26",
                "pocet": 21,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/10/26",
            },
            {
                "datum": "2021-10-29",
                "pocet": 1,
                "odkaz": "https://rozhodnuti.justice.cz/api/opendata/2020/10/29",
            },
        ],
    )
    days = utils.get_days_for_month(2021, 10)
    assert days == [date(2021, 10, 20), date(2021, 10, 26), date(2021, 10, 29)]
