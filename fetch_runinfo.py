#!/usr/bin/env python3

import sys, os, shutil, subprocess, logging, redis, re, pprint

pp = pprint.PrettyPrinter(indent=4)

# Set logging level; log everything.
logging.basicConfig(level=logging.INFO)

# Connect to Redis fargen-db database.
redis_connection = redis.Redis(host='fargen-db', port=6379, db=0)

cursor = 0
while True:
    cursor, results = redis_connection.scan(cursor, match='run_id:*')

    for db_key in results:
        fields = redis_connection.hgetall(db_key)
        pp.pprint(fields)

    if cursor == 0:
        break

