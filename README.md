# ETL Pipeline

## Overview
Kafka Source - Flink Transformations - Postgres Sink

Welcome to the **ETL Pipeline** repository. This experimental project is designed to load a real-time Apache Kafka stream of large dataset csv records (Test data of 100K records for demo) to a JDBC sink (Postgres) via Apache Flink while applying minor transformations. It is built with Java 17, Gradle and Docker, aiming to provide a template code-base for ETL pipelines with large datasets via low latency streaming (some Use Cases listed below). This takes full advantage of local machine's resources and convenient for Proof-Of-Concept demos, learning, experimentation etc.

[//]: # ([Optional: Include a screenshot or a gif of the project])

## Table of Contents

- [Real-World Use Cases](#real-world-use-cases)
- [Future Work](#future-work)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [Contact](#contact)

[//]: # (## Features)

[//]: # ()
[//]: # (- **Feature 1:** Detailed explanation of feature 1.)

[//]: # (- **Feature 2:** Detailed explanation of feature 2.)

[//]: # (- **Feature 3:** Detailed explanation of feature 3.)

## Real-World Use Cases

1. **Real-Time Analytics Dashboard** - Powers a live dashboard for monitoring website traffic and user behavior.
2. **Fraud Detection System** - Detects and flags fraudulent activities in real-time financial transactions.
3. **IoT Data Processing** - Processes and analyzes real-time data from IoT devices for smart city applications.
4. **Log Aggregation and Monitoring** - Aggregates and monitors logs from multiple services to identify critical issues.
5. **Real-Time Inventory Management** - Manages inventory levels in real-time across multiple locations or warehouses.
6. **User Personalization and Recommendations** - Delivers personalized content and recommendations based on user interactions.
7. **Real-Time Financial Market Data Processing** - Analyzes financial market data streams to inform trading strategies in real-time.


## Future Work

1. **Pulling DB Credentials from Secrets Manager** - Implement secure management of database credentials using a Secrets Manager to enhance security and simplify credential rotation.
2. **End-to-End Encrypted Streams** - Develop end-to-end encryption for data streams to ensure the security and privacy of sensitive datasets throughout the entire pipeline.
3. **Enhanced Parser Support** - Extend support to various data formats such as CSV, TSV, Delimited, JSON, and XML, including efficient handling of large datasets during parsing.
4. **On-the-Fly Lookup Support** - Implement on-the-fly lookups to fetch required identifiers and metadata from the database dynamically during stream processing.
5. **Advanced Caching Mechanisms** - Introduce multiple layers of caching to optimize the performance of data transformations and mappings, reducing latency and improving throughput.
6. **Multi-Stream and Multi-Topic Support** - Enable the application to handle multiple Kafka topics and streams concurrently, maximizing resource utilization and scaling to accommodate billions of records per second.
7. **Dynamic Resource Allocation** - Develop a dynamic resource allocation system that scales processing resources up or down based on the workload to ensure efficient resource use and cost management.
8. **Real-Time Anomaly Detection** - Incorporate machine learning models for real-time anomaly detection to identify and respond to unusual patterns and behaviors instantly.
9. **Comprehensive Monitoring and Alerting** - Implement a comprehensive monitoring and alerting system to track the health and performance of the entire data pipeline, ensuring quick detection and resolution of issues.
10. **Enhanced Fault Tolerance and Recovery** - Improve fault tolerance mechanisms and develop automated recovery procedures to ensure the robustness and reliability of the system under various failure scenarios.
11. **Database Agnostic** - Support for variety of databases (not just SQL based) with connection pooling to maximize concurrency

## Installation

### Prerequisites

Before you begin, ensure you have met the following requirements:
- You have installed Java 17 or higher.
- You have installed Docker.
- You have installed Netcat.
- You have installed Git.

[//]: # (- You have a [OS type] machine. [Specify any OS-specific instructions if necessary].)

### Steps

1. Clone the repository:
    ```bash
    git clone https://github.com/lumos9/etl-pipeline.git
    ```
2. Navigate to the project directory:
    ```bash
    cd etl-pipeline
    ```
3. Set up the pipeline
    ```bash
    ./setup.sh
    ```
4. Build the project
    ```bash
    ./gradlew clean build
     ```

## Usage

To use this project, follow these steps:

1. Start the Kafka Consumer one shell:
    ```bash
    ./start-kafka-consumer-flink-db-sink.sh
    ```
2. Start the Kafka Producer in another shell:
    ```bash
    ./start-kafka-producer.sh
    ```

[//]: # (Example:)

[//]: # (```bash)

[//]: # ([example command or code snippet])

[//]: # (```)

[//]: # (## Configuration)

[//]: # ()
[//]: # (### Environment Variables)

[//]: # ()
[//]: # (This project requires the following environment variables to be set:)

[//]: # ()
[//]: # (- `ENV_VAR_1`: Description of ENV_VAR_1)

[//]: # (- `ENV_VAR_2`: Description of ENV_VAR_2)

[//]: # ()
[//]: # (### Configuration File)

[//]: # ()
[//]: # (You can configure the project by editing the `config.file` located at `[path to config file]`. Below is an example configuration:)

[//]: # ()
[//]: # (```json)

[//]: # ({)

[//]: # (  "config_key_1": "value",)

[//]: # (  "config_key_2": "value")

[//]: # (})

[//]: # (```)

## Contributing

We welcome contributions!

### Reporting Issues

If you encounter any issues, please create a new issue in this repository. Make sure to provide enough detail for us to understand and replicate the issue.

### Pull Requests

1. Fork the repository.
2. Create a new branch:
    ```bash
    git checkout -b feature/your-feature-name
    ```
3. Make your changes and commit them:
    ```bash
    git commit -m "Add feature/your-feature-name"
    ```
4. Push to your branch:
    ```bash
    git push origin feature/your-feature-name
    ```
5. Open a pull request.

[//]: # (Please ensure your code adheres to our coding standards and includes appropriate tests.)

[//]: # (## License)

[//]: # ()
[//]: # (This project is licensed under the [LICENSE NAME]. See the [LICENSE]&#40;LICENSE&#41; file for more details.)

## Contact

For any inquiries or questions, please contact me at [contact information].

[//]: # (---)

[//]: # ()
[//]: # (Thank you for checking out **ETL Pipeline**! We hope you find it useful and engaging. Happy coding!)

[//]: # ()
[//]: # ([Optional: Include any acknowledgments or credits here])

[//]: # ()
[//]: # (---)

[//]: # (*Note: Replace placeholders with actual information relevant to your project.*)