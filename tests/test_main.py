import sys
from argparse import Namespace
from pathlib import Path
import pytest
import pandas as pd

import main


@pytest.fixture()
def csv_weather_data(tmpdir) -> Path:
    csv_file = Path(tmpdir.mkdir('sub').join('weather.csv'))
    csv_file.write_text(
        'ForecastSiteCode,ObservationTime,ObservationDate,WindDirection,WindSpeed,WindGust,Visibility,'
        'ScreenTemperature,Pressure,SignificantWeatherCode,SiteName,Latitude,Longitude,Region,Country\n'
        '3002,0,2016-02-01T00:00:00,12,8,,30000,2.10,997,8,BALTASOUND (3002),60.7490,-0.8540,Orkney & Shetland,SCOTLAND,\n'
        '3005,0,2016-02-01T00:00:00,10,2,,35000,0.10,997,7,LERWICK (S. SCREEN) (3005),60.1390,-1.1830,Orkney & Shetland,SCOTLAND,\n'
        '3008,0,2016-02-01T00:00:00,8,6,,50000,2.80,997,-99,FAIR ISLE (3008),59.5300,-1.6300,Orkney & Shetland,\n'
    )
    return csv_file


@pytest.fixture()
def sorted_df_by_hottest_day() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            'ScreenTemperature': [2.1, 1.6, 0.1],
            'Region': ['Orkney & Shetland', 'Grampian', 'Eilean Siar'],
            'ObservationDate': ['2016-02-01T00:00:00', '2016-02-01T00:00:00', '2016-02-01T00:00:00'],
            'Pressure': [997, 996, 997]
        }
    )
    return df


class TestMainHottestDay:
    def test_arg_parser(self, mocker):
        mocker.patch('sys.argv')
        sys.argv = ['program.py', '-d', './input_feeds/foobar.csv']
        expected_args = Namespace(
            data='./input_feeds/foobar.csv',
            output='./weather_data.parquet',
        )
        args = main.arg_parser()
        assert args == expected_args

    def test_read_source_data(self, csv_weather_data):
        outcome = main.read_source_data(csv_weather_data)
        assert isinstance(outcome, pd.DataFrame)
        assert outcome.shape == (3, 15)

    def test_sort_df(self, sorted_df_by_hottest_day):
        df = pd.DataFrame(
            {
                'ScreenTemperature': [2.1, 0.1, 1.6],
                'Region': ['Orkney & Shetland', 'Eilean Siar', 'Grampian'],
                'ObservationDate': ['2016-02-01T00:00:00', '2016-02-01T00:00:00', '2016-02-01T00:00:00'],
                'Pressure': [997, 997, 996]
            }
        )
        outcome = main.sort_df(df_obj=df, columns=['ScreenTemperature'], ascending=False)
        assert outcome.equals(sorted_df_by_hottest_day)

    def test_get_hottest_day_info(self, sorted_df_by_hottest_day):
        outcome = main.get_hottest_day_info(sorted_df_by_hottest_day, top_results=1)
        expected = pd.DataFrame(
            {
                'ObservationDate': ['2016-02-01'],
                'ScreenTemperature': [2.1],
                'Region': ['Orkney & Shetland'],
            }
        )
        assert outcome.equals(expected)

    def test_write_df_to_parquet(self, mocker, tmp_path, sorted_df_by_hottest_day):
        mock_to_parquet = mocker.patch.object(pd.DataFrame, 'to_parquet')
        main.write_df_to_parquet(sorted_df_by_hottest_day, file_path=tmp_path)
        mock_to_parquet.assert_called_once()
