import pandas as pd
import argparse
from pathlib import Path
from typing import List


def arg_parser() -> argparse.Namespace:
    """Process command line arguments
    """
    parser = argparse.ArgumentParser(description='given a weather data set, displays information about the hottest day')
    parser.add_argument('-o', '--output', dest='output',
                        help='Output src data to parquet format, default: ./weather.parquet',
                        default='./weather_data.parquet')
    required_arg = parser.add_argument_group('Required named arguments')
    required_arg.add_argument('-d', '--data', dest='data', required=True,
                              help='path to csv file containing weather data')
    return parser.parse_args()


def read_source_data(csv_file: Path) -> pd.DataFrame:
    """Reads CSV file into a Pandas dataframe object
    """
    df = pd.read_csv(
        filepath_or_buffer=csv_file,
        delimiter=',',
        compression=None,
        # header=None,
        engine='python'
    )
    return df


def sort_df(df_obj: pd.DataFrame, columns: List, ascending: bool = False) -> pd.DataFrame:
    """Sorts dataframe object by the given list of columns
    """
    df = df_obj.sort_values(
        by=columns,
        axis=0,
        ascending=ascending,
        inplace=False
    )
    return df.reset_index(drop=True)


def get_hottest_day_info(df_obj: pd.DataFrame, top_results: int = 1) -> pd.DataFrame:
    """Gets top n rows of dataframe for hottest day
    (Assuming dataframe is already sorted by temperature)
    """
    df_obj.loc[:, 'ObservationDate'] = df_obj['ObservationDate'].apply(lambda x: x[:10])
    df_obj = df_obj.head(top_results)
    return df_obj[['ObservationDate', 'ScreenTemperature', 'Region']]


def write_df_to_parquet(df_obj: pd.DataFrame, file_path: Path) -> None:
    """Writes the contents of a dataframe object to a Parquet file.
    """
    df_obj.to_parquet(file_path, compression=None, engine='auto')
    print(f'Parquet file written: {file_path}')
    return


def main():
    args = arg_parser()
    df = read_source_data(args.data)
    write_df_to_parquet(df_obj=df, file_path=args.output)
    df_highest_temps = sort_df(df_obj=df, columns=['ScreenTemperature'])
    df_hottest_day = get_hottest_day_info(df_highest_temps)
    print('\n', df_hottest_day, '\n')


if __name__ == '__main__':
    main()
