#!/usr/bin/env python

"""
BigQuery Rank:
    Computes a new table in bigquery with the rank with respect to
    a specified field.

    1. Extract the bigquery src_table to storage in a specified bucket
    2. Download the files in /tmp
    3. Uncompress files - Sort - Rank
    4. Upload ranked file to bigquery

Usage:
  bigquery_rank.py <project> <bucket> <dataset> <src_table> <dst_table> <field> [--reverse] [--numerical]
  bigquery_rank.py -h | --help
"""
from docopt import docopt
from google.cloud import bigquery, storage
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.schema import SchemaField
import random
import string
import subprocess
import time

JOBID_ALLOWED_CHARS = string.ascii_letters + string.digits + '_'


def generate_random_string(n_chars=128):
    return ''.join(random.choice(JOBID_ALLOWED_CHARS) for _ in xrange(n_chars))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(30)


def get_bigquery_client(project_id):
    return bigquery.Client(project=project_id)


def get_storage_client(project_id):
    return storage.Client(project=project_id)


def get_dataset_object(dataset_id, client):
    return Dataset(dataset_id, client)


def get_table_object(table_id, dataset, *args):
    return Table(table_id, dataset, *args)


def get_bucket_object(bucket_id, client):
    return client.bucket(bucket_id)


def extract_bigquery_table_to_storage(table, bucket_id, bigquery_client):
    job_id = generate_random_string()
    gcs_filename = generate_random_string()
    destination_uris = 'gs://{}/{}*'.format(bucket_id, gcs_filename)

    job = bigquery_client.extract_table_to_storage(job_id, table, destination_uris)
    job.print_header = False
    job.compression = 'GZIP'
    job.begin()
    wait_for_job(job)
    return gcs_filename


def get_table_schema(src_table):
    src_table.reload()  # necessary to get the schema
    schema = src_table.schema
    return schema


def get_bigquery_column_index(schema, field_id):
    # sort command-line indexing starts at 1
    column_sort_key = str(1 + [s.name for s in schema].index(field_id))
    return column_sort_key


def get_rank_table_schema(src_schema):
    dst_schema_field = SchemaField('rank', 'INTEGER', mode='REQUIRED')
    dst_schema = [dst_schema_field] + src_schema
    return dst_schema


def download_storage_blobs(gcs_filename, bucket):
    tmp_filename = '/tmp/{}'.format(generate_random_string(128))
    zipped_tmp_filename = tmp_filename + '.gz'
    with open(zipped_tmp_filename, 'wb') as file_obj:
        for blob in bucket.list_blobs(prefix=gcs_filename):
            blob.download_to_file(file_obj)
        bucket.delete_blob(blob.name)
    return tmp_filename


def create_ranked_file(tmp_filename, column_sort_key, reverse, numerical):
    # unzip file
    zipped_tmp_filename = tmp_filename + '.gz'
    subprocess.check_call(['gunzip', zipped_tmp_filename])

    # define sorting process
    sort_args = ["sort"]
    if reverse:
        sort_args.append("-r")
    if numerical:
        sort_args.append("-n")
    sort_args += ["-k", column_sort_key, "-t", ",", tmp_filename]
    sort_process = subprocess.Popen(sort_args, stdout=subprocess.PIPE)

    # pipe process into a ranking process so that the lines are numbered
    rank_args = ['nl', '-s', ',']
    ranked_file_name = tmp_filename + '_ranked'
    file_to_write = open(ranked_file_name, 'w')
    subprocess.check_call(rank_args, stdin=sort_process.stdout, stdout=file_to_write)

    # check if errors occured while sorting
    returncode = sort_process.wait()
    if returncode != 0:
        raise Exception("sort failed with return %d" % returncode)
    return ranked_file_name


def upload_ranked_file_to_bigquery(ranked_file_name, dst_table):
    if not dst_table.exists():
        dst_table.create()
    job = dst_table.upload_from_file(
        open(ranked_file_name, 'rb'),
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
    )
    wait_for_job(job)


def main():
    # get input arguments
    arguments = docopt(__doc__)
    project_id = arguments['<project>']
    dataset_id = arguments['<dataset>']
    bucket_id = arguments['<bucket>']
    src_table_id = arguments['<src_table>']
    dst_table_id = arguments['<dst_table>']
    field_id = arguments['<field>']
    reverse = arguments['--reverse']
    numerical = arguments['--numerical']

    print 'Extract source bigquery table to storage ..'
    bigquery_client = get_bigquery_client(project_id)
    dataset = get_dataset_object(dataset_id, bigquery_client)
    src_table = get_table_object(src_table_id, dataset)
    gcs_filename = extract_bigquery_table_to_storage(
        src_table,
        bucket_id,
        bigquery_client
    )

    print 'Download to disk ..'
    storage_client = get_storage_client(project_id)
    bucket = get_bucket_object(bucket_id, storage_client)
    tmp_filename = download_storage_blobs(
        gcs_filename,
        bucket
    )

    print 'Rank ..'
    src_schema = get_table_schema(src_table)
    column_sort_key = get_bigquery_column_index(src_schema, field_id)
    ranked_file_name = create_ranked_file(
        tmp_filename,
        column_sort_key,
        reverse,
        numerical
    )

    print 'Upload ranked file to destination table ..'
    dst_schema = get_rank_table_schema(src_schema)
    dst_table = get_table_object(dst_table_id, dataset, dst_schema)
    upload_ranked_file_to_bigquery(
        ranked_file_name,
        dst_table,
    )


if __name__ == '__main__':
    main()
