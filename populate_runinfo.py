#!/usr/bin/env python3

import sys, os, shutil, subprocess, logging, redis, re

# Set some constants.
# NOTE: these may be subject to change, make sure they are correct.
SAMPLESHEETS_DIR = '/fargen/data/samplesheets'
FASTQ_DIR = '/fargen/data/fastq'
SEQUENCER_NAME = 'NS500347'

# Set logging level; log everything.
logging.basicConfig(level=logging.INFO)

# Connect to Redis fargen-db database.
redis_connection = redis.Redis(host='fargen-db', port=6379, db=0)

# Get contents of samplesheet directory.
dir_contents = os.listdir(SAMPLESHEETS_DIR)

# Use regex to filter out anything that isn't a samplesheet.
# FIXME: this pattern is quite stringent. Perhaps make it more lenient.
pattern = re.compile('^[0-9]{6}_%s_[0-9]{4}_[A-Z0-9]{10}(_[0-9]+)*.csv' % SEQUENCER_NAME)
samplesheet_list = [x for x in dir_contents if pattern.match(x)]

# Obtain run IDs by clipping of the file extension.
# NOTE: This will only work as long as the extension is of length 4 as in the regex above.
run_ids = [x[:-4] for x in samplesheet_list]

# Keep a tally of how many items were added to samplesheet.
runids_added = 0
ss_added = 0
reports_added = 0
for run_id in run_ids:
    db_key = 'run_id:' + run_id

    runids_added += redis_connection.hset(db_key, key='run_id', value=run_id)

    # Construct path for samplesheet.
    samplesheet_path = '%s/%s.csv' % (SAMPLESHEETS_DIR, run_id)
    # Make sure the file exists.
    # NOTE: if this fails, something went terribly wrong, as we got the run ID from the samplesheet earlier.
    assert os.path.isfile(samplesheet_path), 'Samplesheet for run %s not found: %s' % (run_id, samplesheet_path)
    # When adding the samplesheet to the DB, we increment the counter.
    # If this key did not already exist, the counter is incremented.
    ss_added += redis_connection.hset(db_key, key='samplesheet', value=samplesheet_path)
    # NOTE: if the key existed, but was overwritten by a new key, the counter is not incremented.

    demux_report_path = '%s/%s/multiqc/multiqc_report.html' % (FASTQ_DIR, run_id)
    if os.path.isfile(demux_report_path):
        reports_added += redis_connection.hset(db_key, key='demux-report', value=demux_report_path)
        #logging.info('Adding demux report %s to run %s.' % (demux_report_path, run_id))
    else:
        logging.info('Demux report for run %s was not found on path %s.' % (run_id, demux_report_path))

print('Added %d run IDs, %d samplesheets and %d reports to database.' % (runids_added, ss_added, reports_added))
