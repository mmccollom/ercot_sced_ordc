from datetime import datetime, timedelta
from ercotutils import misutil
import io
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz


def execute():
    report_type_id = '13221'
    s3_path = 's3://ercot-62841215/sced_ordc/'

    # create cut_off_dt from publish_date and publish_hour
    local_tz = pytz.timezone('America/Chicago')
    from_date = datetime.now(local_tz) - timedelta(days=1,
                                                   hours=datetime.now(local_tz).hour,
                                                   minutes=datetime.now(local_tz).minute,
                                                   seconds=datetime.now(local_tz).second,
                                                   microseconds=datetime.now(local_tz).microsecond)
    to_date = datetime.now(local_tz) - timedelta(hours=datetime.now(local_tz).hour,
                                                 minutes=datetime.now(local_tz).minute,
                                                 seconds=datetime.now(local_tz).second,
                                                 microseconds=datetime.now(local_tz).microsecond)

    print(f'Date range: {from_date} to {to_date}')

    # get document list from ERCOT
    documents_dict = misutil.get_ice_doc_list(report_type_id)

    # load documents into dataframe
    df = pd.json_normalize(documents_dict)
    df['Document.PublishDate'] = pd.to_datetime(df['Document.PublishDate'], format='%Y-%m-%dT%H:%M:%S%z')

    # create a new column for y/m/d as a str of publish date
    df['Document.PublishDateStr'] = df['Document.PublishDate'].dt.strftime('%Y-%m-%d')

    # create a new column for the Hour of publish date
    df['Document.PublishHour'] = df['Document.PublishDate'].dt.hour

    # filter dataframe to remove files published prior to cut_off_dt
    df = df[(df['Document.PublishDate'] >= from_date) & (df['Document.PublishDate'] < to_date)
            & (df['Document.FriendlyName'].str.endswith('csv'))]
    # export dataframe to json
    documents_dict = json.loads(df.to_json(orient='records'))

    # assert documents_dict is not None
    assert documents_dict is not None, f'No documents found for report_type_id: {report_type_id}'

    df = None

    for document in documents_dict:
        document_id = document['Document.DocID']
        document_content = misutil.get_zipped_file_contents(document_id).decode('utf-8')

        # read document_content into buffer
        document_content = io.StringIO(document_content)

        # read bytes into dataframe
        tmp_df = pd.read_csv(document_content)

        if df is None:
            df = tmp_df
        else:
            df = df.append(tmp_df, ignore_index=True)

    # end for loop over documents_dict

    # trim extra whitespace from column names
    df.columns = df.columns.str.strip()

    # rename columns
    col_remap = {'SCEDTimestamp': 'effective_from', 'RepeatedHourFlag': 'is_day_light_savings',
                 'SystemLambda': 'system_lambda'}
    df.rename(columns=col_remap, inplace=True)

    # drop BatchID
    df.drop(columns=['BatchID'], inplace=True)

    # convert effective_from to str and then to datetime, format being 05/19/2023 23:55:18
    df['effective_from'] = pd.to_datetime(df['effective_from'], format='%m/%d/%Y %H:%M:%S')

    # create effective_to column by adding 5 minutes to effective_from
    df['effective_to'] = df['effective_from'] + timedelta(minutes=5)

    # round effective_from down to nearest 5 minute interval
    df['effective_from'] = df['effective_from'].dt.floor('5min')

    # round effective_to down to nearest 5 minute interval
    df['effective_to'] = df['effective_to'].dt.floor('5min')

    # reformat delivery_date str in format YYYY-MM-DD
    df['delivery_date'] = df['effective_from'].dt.strftime('%Y-%m-%d')

    # sort by effective_from
    df.sort_values(by=['effective_from'], inplace=True)

    # write dataframe to s3 using pyarrow
    table = pa.Table.from_pandas(df=df)
    pq.write_to_dataset(table=table, root_path=s3_path, compression='snappy',
                        partition_cols=['delivery_date'])


def lambda_handler(event, context):
    print("In Lambda Handler")
    execute()


if __name__ == "__main__":
    print("In Main")
    execute()
