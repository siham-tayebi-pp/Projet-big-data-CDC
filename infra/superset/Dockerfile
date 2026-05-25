FROM apache/superset:3.1.3

USER root
# sqlalchemy-trino : driver Python pour connecter Superset à Trino
RUN pip install --no-cache-dir trino sqlalchemy-trino

COPY create_admin.sh /app/docker/docker-init.d/create_admin.sh
RUN chmod +x /app/docker/docker-init.d/create_admin.sh

USER superset
