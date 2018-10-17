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

ilo = curator.IndexList(client)
ilo.filter_by_regex(kind="prefix", value=prefix)
ilo.filter_by_age(source="name", direction="older", timestring="%Y.%m.%d", unit="days", unit_count=days)

if not ilo.indices:
  print('\nNo matching indices found with days:', days, "prefix:", prefix)
  sys.exit(0)

print('Running curator prefix:', prefix, "days:", days, "action:", action, "affected indices:", ilo.indices)

if action == "DELETE":
  delete_indices = curator.DeleteIndices(ilo)
  delete_indices.do_action()
  print("\n")
  print("Index List")
  for index in client.indices.get("*"):
    print(index)
