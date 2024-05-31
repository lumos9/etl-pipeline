#!/bin/bash

#HOST="localhost"
#PORT="5432"
#USER="postgres"
#PASSWORD="password"
#DB_NAME="postgres"

#function check_db_ready() {
#  PGPASSWORD=$PASSWORD psql -h $HOST -p $PORT -U $USER -d $DB_NAME -c '\q' > /dev/null 2>&1
#  return $?
#}

echo "Waiting for the database to be ready..."
until pg_isready; do
  sleep 1
  echo -n "."
done

echo "Database is ready!"