FROM postgres:latest

# Switch to root to install dependencies
USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-all \
    curl \
    libcurl4-openssl-dev \
    unzip

# Create a directory for the extension source code
RUN mkdir -p /usr/src/pg_cdc_webhook

# Switch to the postgres user
USER postgres

WORKDIR /usr/src/pg_cdc_webhook

# Copy the source code into the container
COPY . /usr/src/pg_cdc_webhook

# Switch back to root for the build step
USER root

# Build the extension
RUN make && make install

# Switch back to postgres user
USER postgres

# Initialize the database cluster
RUN initdb -D "$PGDATA"

# Allow all hosts to connect to the postgres user for testing purposes
RUN echo "host all all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"

# Allow remote connections to PostgreSQL server
RUN echo "listen_addresses='*'" >> "$PGDATA/postgresql.conf"

# Run PostgreSQL server and create database and extensions
CMD pg_ctl -D "$PGDATA" -o "-c listen_addresses='*'" -w start && \
    psql --command "CREATE DATABASE testdb;" && \
    pg_ctl -D "$PGDATA" -m fast -w stop && \
    postgres
