#!/bin/bash

./check-setup.sh

./gradlew clean build

java -cp build/libs/app-1.0.jar org.example.KafkaFlinkRedshift