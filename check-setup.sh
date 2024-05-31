#!/bin/bash

source setup/logger.sh

# Function to print error message and exit
function error_exit {
    error "$1" 1>&2
    exit 1
}

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

# Check if Kafka broker is up and running on localhost port 9092
docker_ps_output=$(docker ps --filter "publish=9092" --format "{{.Names}}")

if [ -z "$docker_ps_output" ]
then
    error_exit "Kafka broker is not running on localhost:9092 in any Docker container."
else
    kafka_container=$(docker exec -it "$docker_ps_output" sh -c 'nc -zv localhost 9092 2>&1')
    if [[ "$kafka_container" == *"succeeded"* || "$kafka_container" == *"Connected to"* ]]
    then
        info "Kafka broker is up and running on localhost:9092 in container: $docker_ps_output"
    else
        error_exit "Kafka broker is not accessible on localhost:9092 in the Docker container."
    fi
fi

info "All checks passed successfully."