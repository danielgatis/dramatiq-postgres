#!/bin/bash -eux

cat >>$PGDATA/postgresql.dramatiq.conf<<EOF
log_statement = 'all'
log_duration = on
EOF

cat >>$PGDATA/postgresql.conf<<EOF
include postgresql.dramatiq.conf
EOF
