FROM quay.io/debezium/connect:2.7
# L'image Debezium Connect 2.7 inclut déjà le connecteur PostgreSQL.
# On n'a pas besoin d'installer de plugin supplémentaire.
# Le connecteur PostgreSQL utilise "pgoutput", le plugin natif de PG16.

USER root
COPY start-with-connectors.sh /scripts/start-with-connectors.sh
RUN chmod +x /scripts/start-with-connectors.sh
USER kafka
