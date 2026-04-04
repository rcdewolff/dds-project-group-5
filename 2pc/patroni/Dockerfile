FROM postgres:16-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv python3-dev curl libpq-dev gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install patroni in an isolated venv to avoid system package conflicts
RUN python3 -m venv /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]" psycopg2-binary

ENV PATH="/opt/patroni/bin:$PATH"

RUN mkdir -p /data/patroni /etc/patroni \
    && chown -R postgres:postgres /data/patroni /etc/patroni

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER postgres
EXPOSE 5432 8008
ENTRYPOINT ["/entrypoint.sh"]
