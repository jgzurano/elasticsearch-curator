#!/usr/bin/python

import sys
import os

try:
  repo = sys.argv[1]
  days = int(sys.argv[2])
  action = sys.argv[3]
except IndexError:
  print('Missing argument')
  sys.exit(0)

try:
  es_host = os.environ["ES_HOST"]
  es_port = os.environ["ES_PORT"]
  es_user = os.environ["ES_USER"]
  es_pass = os.environ["ES_PASS"]
except IndexError:
  print('Missing env variables')
  sys.exit(0)

import elasticsearch
import curator
from elasticsearch import Elasticsearch

client = Elasticsearch([es_host],
                   http_auth=(es_user, es_pass),
                   port=es_port,
                   timeout=300
                  )

try:
  slo = curator.SnapshotList(client,repository=repo)
  slo.filter_by_age(source='creation_date', direction="older", timestring=None, unit="days", unit_count=days, epoch=None, exclude=False)
except:
  print("No snapshots found older than:",days,"repo:",repo)
  sys.exit(0)

if not slo.snapshots:
  print("No snapshots found older than:",days,"repo:",repo)
  sys.exit(0)

print('Running curator snapshot repository:', repo, "days:", days, "action:", action)
print('Affected snapshots')
for snap in slo.snapshots:
  print(snap)

delete_snapshots = curator.DeleteSnapshots(slo)

if action == "DELETE":
  delete_snapshots.do_action()
else:
  delete_snapshots.do_dry_run()
