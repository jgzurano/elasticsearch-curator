#!/usr/bin/python

import sys
import os

try:
  prefix = sys.argv[1]
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
                   timeout=60
                  )

try:
  ilo = curator.IndexList(client)
  ilo.filter_by_regex(kind="prefix", value=prefix)
  ilo.filter_by_age(source="name", direction="older", timestring="%Y.%m.%d", unit="days", unit_count=days)
except:
  print("No indices found with prefix:",prefix)
  sys.exit(0)

if not ilo.indices:
  print("No indices older than", days,"days with prefix:", prefix)
  sys.exit(0)

print('Running curator prefix:', prefix, "days:", days, "action:", action)
print('Affected indices')
for index in ilo.indices:
  print(index)

delete_indices = curator.DeleteIndices(ilo)

if action == "DELETE":
  delete_indices.do_action()
else:
  delete_indices.do_dry_run()
