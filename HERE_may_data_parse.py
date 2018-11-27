"""
HERE_may_data_parse.py
By Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov

One-time use script to calculate average travel times and segment lengths for
non-Memorial Day Tu, W, Thu in May. Uses HERE data.
"""

import pandas as pd
import datetime as dt
import os

drive_path = 'C:/Users/saavedrak/metro_work/HERE_sandbox/data/'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)


def tt_by_hour(df_tt, hour):
    """Process hourly travel time averages."""
    df_tt = df_tt[df_tt['utc_time_id'].dt.hour.isin([hour])]
    tmc_operations = ({'avg_travel_time': 'mean'})
    df_tt = df_tt.groupby('source_id', as_index=False).agg(tmc_operations)

    df_tt['tt_secs'] = df_tt['avg_travel_time'] * 60
    df_tt = df_tt.drop(columns=['avg_travel_time'])
    df_avg_tt = df_tt.rename(
        columns={'tt_secs': 'hour_{}_tt_seconds'.format(hour)})

    return df_avg_tt


def assemble_dataset():
    df_full = pd.DataFrame()
    for file in os.listdir(drive_path):
        print('Loading {}'.format(file))
        df_temp = pd.read_csv(
            os.path.join(os.path.dirname(__file__), drive_path, file),
            usecols=['utc_time_id', 'source_ref', 'source_id',
                     'avg_travel_time', 'avg_speed'])
        df_full = pd.concat([df_full, df_temp])

    return df_full


def main():
    startTime = dt.datetime.now()
    print('Script started at {0}'.format(startTime))

    df = assemble_dataset()
    df['utc_time_id'] = pd.to_datetime(df['utc_time_id'], errors='coerce',
                                       infer_datetime_format=True)

    df = df.loc[df['source_ref'] == 'tmc']
    df = df.dropna(how='all')

    source_id = df['source_id'].drop_duplicates().values.tolist()
    tmc_format = {'source_id': source_id}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_tmc_lengths = pd.read_csv('may_2017_raw_lengths.csv')
    FEET_IN_KM = .621371
    df_tmc_lengths['total_length_miles'] = (df_tmc_lengths['total_length_km'] *
                                            FEET_IN_KM)

    hours = list(range(0, 24))
    for hour in hours:
        df_time = tt_by_hour(df, hour)
        df_tmc = pd.merge(df_tmc, df_time, on='source_id', how='left')

    df_final = pd.merge(df_tmc, df_tmc_lengths, on='source_id', how='left')
    df_final.to_csv('may_2017.csv', index=False)

    endTime = dt.datetime.now()
    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
