#!/bin/bash

#!/bin/bash

# Function to print error message and exit
function error_exit {
    echo "$1" 1>&2
    exit 1
}

# Function to run a command and check its exit code
run_command() {
    "$@"
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        # shellcheck disable=SC2145
        echo "Error: Command '$@' failed with exit code '$EXIT_CODE'"
        exit $EXIT_CODE
    fi
}

# Check if Docker is installed and print version
if command -v docker &> /dev/null
then
    docker_version=$(docker --version)
    echo "Docker is installed: $docker_version"
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
        echo "Java is installed: version $java_version"
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
        echo "Python is installed: version $python_version"
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
        echo "All services are successfully initiated."
    else
        echo "services failed to start."
        #echo "$status"
        exit 1
    fi
}

# Check if Kafka broker is up and running on localhost port 9092
docker_ps_output=$(docker ps --filter "publish=9092" --format "{{.Names}}")

if [ -z "$docker_ps_output" ]
then
    echo "Kafka broker is not running on localhost:9092 in any Docker container. Starting..."
    COMPOSE_FILE="setup/kafka-docker-compose.yml"
    # Start the services
    run_command docker-compose -f $COMPOSE_FILE up -d --remove-orphans
    # Check the status of the services
    status=$(docker-compose -f $COMPOSE_FILE ps)
    echo "Waiting for 5 secs to initiate Kafka services..."
    countdown 5
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        echo "All services are successfully initiated."
    else
        echo "services failed to start."
        echo "$status"
        exit 1
    fi
    echo "Waiting for 1 minute to load Kafka services..."
    countdown 60
fi

kafka_container=$(docker exec -it "$docker_ps_output" sh -c 'nc -zv localhost 9092 2>&1')
if [[ "$kafka_container" == *"succeeded"* || "$kafka_container" == *"Connected to"* ]]
then
    echo "Kafka broker is up and running on localhost:9092 in container: $docker_ps_output"
else
    echo "Kafka broker is not accessible on localhost:9092 in the Docker container.."
    echo "Waiting for another minute to load Kafka services..."
    countdown 60
    kafka_container=$(docker exec -it "$docker_ps_output" sh -c 'nc -zv localhost 9092 2>&1')
    if [[ "$kafka_container" == *"succeeded"* || "$kafka_container" == *"Connected to"* ]]
    then
        echo "Kafka broker is up and running on localhost:9092 in container: $docker_ps_output"
    else
        echo "Something went wrong launching Confluence kafka via docker container"
        exit 1
    fi
fi


# Variables
KAFKA_CONTAINER_NAME="broker"
TOPIC_NAME="sample-stream"
BROKER_HOST="localhost"
BROKER_PORT="9092"
NUM_PARTITIONS=1
REPLICATION_FACTOR=1

echo "Setting up new kafka topic '$TOPIC_NAME'.."

# Create the Kafka topic
run_command docker exec -it \
  $KAFKA_CONTAINER_NAME \
  kafka-topics \
  --create \
  --topic $TOPIC_NAME \
  --bootstrap-server $BROKER_HOST:$BROKER_PORT \
  --partitions $NUM_PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo "Kafka topic '$TOPIC_NAME' created successfully"

echo "Setting up local postgres db.."

COMPOSE_FILE="setup/postgres-docker-compose.yml"
run_command docker-compose -f $COMPOSE_FILE up -d

run_command docker cp setup/wait-for-postgres-startup.sh local_postgres:/wait-for-postgres-startup.sh

# Execute the readiness check from the host machine
run_command docker exec -it local_postgres /wait-for-postgres-startup.sh

run_command docker cp setup/create_table.sql local_postgres:/create_table.sql

# Check if postgres container is up and running on localhost port 5432
docker_ps_output=$(docker ps --filter "publish=5432" --format "{{.Names}}")
#postgres_container=$(docker exec -it "$docker_ps_output" bash -c 'nc -zv localhost 5432 2>&1')
postgres_db_status=$(nc -zv localhost 5432 2>&1)
if [[ "$postgres_db_status" == *"succeeded"* || "$postgres_db_status" == *"Connected to"* ]]
then
    echo "Postgres DB is already up and running on localhost:5432 in container: $docker_ps_output"
    run_command docker exec -i local_postgres psql -U postgres -d postgres -f /create_table.sql
else
    error_exit "Unable to connect postgres db via container"
fi

echo "setup complete!"