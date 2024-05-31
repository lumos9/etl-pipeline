#!/bin/bash

source setup/logger.sh

# Function to print error message and exit
function error_exit {
    error "$1" 1>&2
    exit 1
}

# Function to run a command and check its exit code
#run_command() {
#    "$@"
#    EXIT_CODE=$?
#    if [ $EXIT_CODE -ne 0 ]; then
#        # shellcheck disable=SC2145
#        error "Command '$@' failed with exit code '$EXIT_CODE'"
#        exit $EXIT_CODE
#    fi
#}

# Check if Docker is installed and print version
if command -v docker &> /dev/null
then
    docker_version=$(docker --version)
    info "Docker is installed: $docker_version"
else
    error_exit "Docker is not installed. Please install Docker and try again."
fi

# Check if Java is installed and ensure it's version 17 or above
if command -v java &> /dev/null
then
    java_version=$(java -version 2>&1 | awk -F[\"_] 'NR==1 {print $2}')
    # shellcheck disable=SC2071
    if [[ "$java_version" < "17" ]]
    then
        error_exit "Java version is $java_version. Please install Java 17 or above."
    else
        info "Java is installed: version $java_version"
    fi
else
    error_exit "Java is not installed. Please install Java 17 or above and try again."
fi

# Check if Python is installed and print version, ensure it's version 3 or above
if command -v python3 &> /dev/null
then
    python_version=$(python3 --version 2>&1 | awk '{print $2}')
    # shellcheck disable=SC2071
    if [[ "$python_version" < "3" ]]
    then
        error_exit "Python version is $python_version. Please install Python 3 or above."
    else
        info "Python is installed: version $python_version"
    fi
else
    error_exit "Python is not installed. Please install Python 3 or above and try again."
fi

# Function to display the countdown
countdown() {
    local seconds=$1
    while [ "$seconds" -gt 0 ]; do
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            echo -ne "$(date -u -r "$seconds" +%H:%M:%S)s\r"
        else
            # Linux/Unix
            echo -ne "$(date -u -d @"$seconds" +%H:%M:%S)s\r"
        fi
        sleep 1
        ((seconds--))
    done
    echo -e "\n"
}

# Function to check if all services are running
check_services() {
    #echo "$status" | grep -q "Exit"
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        info "All services are successfully initiated."
    else
        error "services failed to start."
        #echo "$status"
        exit 1
    fi
}

# Variables
KAFKA_CONTAINER_NAME="broker"
TOPIC_NAME="sample-stream"
BROKER_HOST="localhost"
BROKER_PORT="9092"
NUM_PARTITIONS=1
REPLICATION_FACTOR=1

# Check if Kafka broker is up and running on localhost port 9092
docker_ps_output=$(docker ps --filter "publish=9092" --format "{{.Names}}")

if [ -z "$docker_ps_output" ]
then
    info "Kafka broker is not running on localhost:9092 in any Docker container. Starting..."
    COMPOSE_FILE="setup/kafka-docker-compose.yml"
    # Start the services
    run_command docker-compose -f $COMPOSE_FILE up -d --remove-orphans
    # Check the status of the services
    status=$(docker-compose -f $COMPOSE_FILE ps)
    info "Waiting for 5 secs to initiate Kafka services..."
    countdown 5
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        info "All services are successfully initiated."
    else
        error "services failed to start."
        error "$status"
        exit 1
    fi
    info "Waiting for 1 minute to load Kafka services..."
    countdown 60
fi

# Function to check if a Kafka topic exists
topic_exists() {
    TOPIC_NAME=$1
    docker exec $KAFKA_CONTAINER_NAME kafka-topics --list --bootstrap-server $BROKER_HOST:$BROKER_PORT | grep -w "$TOPIC_NAME" > /dev/null 2>&1
    return $?
}

# Function to delete a Kafka topic if it exists
delete_topic_if_exists() {
    TOPIC_NAME=$1
    info "Checking if Kafka Topic '$TOPIC_NAME' already exists..."
    if topic_exists "$TOPIC_NAME"; then
        info "Kafka Topic '$TOPIC_NAME' already exists. Deleting it..."
        docker exec $KAFKA_CONTAINER_NAME kafka-topics --delete --topic "$TOPIC_NAME" --bootstrap-server $BROKER_HOST:$BROKER_PORT
        if [ $? -eq 0 ]; then
            info "Kafka Topic '$TOPIC_NAME' deleted successfully."
        else
            error_exit "Failed to delete topic '$TOPIC_NAME'."
        fi
    else
        info "Kafka Topic '$TOPIC_NAME' does not exist. No action taken."
    fi
}

kafka_container=$(docker exec -it "$docker_ps_output" sh -c 'nc -zv localhost 9092 2>&1')
if [[ "$kafka_container" == *"succeeded"* || "$kafka_container" == *"Connected to"* ]]
then
    info "Kafka broker is up and running on localhost:9092 in container: $docker_ps_output"
    delete_topic_if_exists $TOPIC_NAME
else
    warn "Kafka broker is not accessible on localhost:9092 in the Docker container.."
    info "Waiting for another minute to load Kafka services..."
    countdown 60
    kafka_container=$(docker exec -it "$docker_ps_output" sh -c 'nc -zv localhost 9092 2>&1')
    if [[ "$kafka_container" == *"succeeded"* || "$kafka_container" == *"Connected to"* ]]
    then
        info "Kafka broker is up and running on localhost:9092 in container: $docker_ps_output"
        delete_topic_if_exists $TOPIC_NAME
    else
        error "Something went wrong launching Confluence kafka via docker container"
        exit 1
    fi
fi

info "Setting up new kafka topic '$TOPIC_NAME'.."

# Create the Kafka topic
run_command docker exec -it \
  $KAFKA_CONTAINER_NAME \
  kafka-topics \
  --create \
  --topic "$TOPIC_NAME" \
  --bootstrap-server $BROKER_HOST:$BROKER_PORT \
  --partitions $NUM_PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

info "Kafka topic '$TOPIC_NAME' created successfully"

info "Setting up local postgres db.."

mkdir -p local_db

info "Stopping existing postgres container 'local_postgres'.."
run_command docker stop local_postgres

info "Deleting existing postgres container 'local_postgres'.."
run_command docker rm local_postgres

COMPOSE_FILE="setup/postgres/postgres-docker-compose.yml"
run_command docker-compose -f $COMPOSE_FILE up -d

run_command docker cp setup/postgres/wait-for-postgres-startup.sh local_postgres:/wait-for-postgres-startup.sh

# Execute the readiness check from the host machine
run_command docker exec -it local_postgres /wait-for-postgres-startup.sh

run_command docker cp setup/postgres/create_table.sql local_postgres:/create_table.sql

# Check if postgres container is up and running on localhost port 5432
docker_ps_output=$(docker ps --filter "publish=5432" --format "{{.Names}}")
#postgres_container=$(docker exec -it "$docker_ps_output" bash -c 'nc -zv localhost 5432 2>&1')
postgres_db_status=$(nc -zv localhost 5432 2>&1)
if [[ "$postgres_db_status" == *"succeeded"* || "$postgres_db_status" == *"Connected to"* ]]
then
    info "Postgres DB is already up and running on localhost:5432 in container: $docker_ps_output"
    run_command docker exec -i local_postgres psql -U postgres -d postgres -f /create_table.sql
else
    error_exit "Unable to connect postgres db via container"
fi

info "setup complete!"