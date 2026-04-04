#!/bin/bash
set -e

# Write a small post-bootstrap script with the DB name baked in so
# it can be called by patroni after the cluster is first initialised.
cat > /tmp/post_bootstrap.sh << SCRIPT
#!/bin/bash
psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" \
    | grep -q 1 \
    || psql -U postgres -c "CREATE DATABASE \"${POSTGRES_DB}\" WITH ENCODING='UTF8';"
echo "post_bootstrap: database '${POSTGRES_DB}' ready."
SCRIPT
chmod +x /tmp/post_bootstrap.sh

# Generate the full Patroni config from environment variables.
# PATRONI_NAME   – unique node name, e.g. order-db-1
# PATRONI_SCOPE  – cluster name,     e.g. order-cluster
# PATRONI_ETCD_HOST / PATRONI_ETCD_PORT – etcd endpoint
cat > /etc/patroni/patroni.yml << EOF
scope: ${PATRONI_SCOPE:-default-cluster}
namespace: /db/
name: ${PATRONI_NAME}

restapi:
  listen: 0.0.0.0:8008
  connect_address: ${PATRONI_NAME}:8008

etcd3:
  host: ${PATRONI_ETCD_HOST:-etcd}:${PATRONI_ETCD_PORT:-2379}

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 1048576
    postgresql:
      use_pg_rewind: true
      parameters:
        max_connections: 200
        max_replication_slots: 10
        max_wal_senders: 10
        wal_level: replica
        hot_standby: "on"
        wal_keep_size: 128

  initdb:
    - encoding: UTF8
    - data-checksums

  post_bootstrap: /tmp/post_bootstrap.sh

  pg_hba:
    - local  all         all                   trust
    - host   all         all  0.0.0.0/0         md5
    - host   replication replicator 0.0.0.0/0  md5

  users:
    postgres:
      password: ${POSTGRES_PASSWORD:-postgres}
      options:
        - createrole
        - createdb
    replicator:
      password: ${REPLICATION_PASSWORD:-replicator}
      options:
        - replication

postgresql:
  listen: 0.0.0.0:5432
  connect_address: ${PATRONI_NAME}:5432
  data_dir: /data/patroni
  pgpass: /tmp/pgpass0
  authentication:
    replication:
      username: replicator
      password: ${REPLICATION_PASSWORD:-replicator}
    superuser:
      username: postgres
      password: ${POSTGRES_PASSWORD:-postgres}
    rewind:
      username: postgres
      password: ${POSTGRES_PASSWORD:-postgres}
  parameters:
    unix_socket_directories: '/tmp'

tags:
  nofailover: false
  noloadbalance: false
  clonefrom: false
  nosync: false
EOF

exec patroni /etc/patroni/patroni.yml
