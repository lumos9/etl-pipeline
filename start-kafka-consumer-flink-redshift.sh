#!/bin/bash

./check-setup.sh
# Check the exit code of the internal script
if [ $? -ne 0 ]; then
  exit 1
fi

java -cp build/libs/app-1.0.jar org.example.KafkaFlinkRedshift