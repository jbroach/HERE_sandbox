"""
HERE_may_data_parse.py
By Kevin Saavedra, Metro, kevin.saavedra@oregonmetro.gov

One-time use script to calculate average travel times and segment lengths for
non-Memorial Day Tu, W, Thu in May. Uses HERE data.
"""

import pandas as pd
import datetime as dt
import numpy as np
import os

drive_path = 'C:/Users/saavedrak/metro_work/HERE_sandbox/data/'


def tt_by_hour(df_tt, hour):
    """Process hourly travel time averages."""
    df_tt = df_tt[df_tt['PDT'].dt.hour.isin([hour])]
    tmc_operations = ({'avg_travel_time': 'mean',
                       'hour_{0}_5th_pct'.format(hour).format(hour): lambda x: np.percentile(x, 5),
                       'hour_{0}_95th_pct'.format(hour).format(hour): lambda x: np.percentile(x, 95)})
    
    df_tt = df_tt.groupby('source_id', as_index=False).agg(tmc_operations)
    SECS_IN_MIN = 60
    df_tt['tt_secs'] = df_tt['avg_travel_time'] * SECS_IN_MIN
    df_tt['hour_{0}_5th_pct'.format(hour).format(hour)] = df_tt['hour_{0}_5th_pct'.format(hour).format(hour)] * SECS_IN_MIN
    df_tt['hour_{0}_95th_pct'.format(hour).format(hour)] = df_tt['hour_{0}_95th_pct'.format(hour).format(hour)] * SECS_IN_MIN
    df_tt = df_tt.drop(columns=['avg_travel_time'])
    df_avg_tt = df_tt.rename(
        columns={'tt_secs': 'hour_{}_tt_seconds'.format(hour)})

    return df_avg_tt


def assemble_dataset():
    """Assembles all files in /data folder into one dataset"""
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

    # Convert UTC to PDT (GMT-7)
    df['utc_time_id'] = pd.to_datetime(df['utc_time_id'], errors='coerce',
                                       infer_datetime_format=True)
    df.index = pd.to_datetime(df['utc_time_id'], errors='coerce')
    df.index = df.index.tz_localize('UTC')
    df['PDT'] = df.index.tz_convert('PST8PDT')

    #df = df.loc[df['source_ref'] == 'tmc']
    df = df.dropna(how='all')

    source_id = df['source_id'].drop_duplicates().values.tolist()
    tmc_format = {'source_id': source_id}
    df_tmc = pd.DataFrame.from_dict(tmc_format)

    df_tmc_lengths = pd.read_csv('may_2017_raw_lengths.csv')
    MILES_IN_KM = 0.621371
    df_tmc_lengths['total_length_miles'] = (df_tmc_lengths['total_length_km'] *
                                            MILES_IN_KM)

    hours = list(range(0, 24))
    for hour in hours:
        df['hour_{0}_95th_pct'.format(hour)] = df['avg_travel_time']
        df['hour_{0}_5th_pct'.format(hour)] = df['avg_travel_time']
        df_time = tt_by_hour(df, hour)
        df_tmc = pd.merge(df_tmc, df_time, on='source_id', how='left')

    #df_final = pd.merge(df_tmc_lengths, df_tmc, on='source_id', how='left')
    #df_final.to_csv('may_2017_HERE_raw.csv', index=False)
    
    endTime = dt.datetime.now()

    print("Script finished in {0}.".format(endTime - startTime))


if __name__ == '__main__':
    main()
