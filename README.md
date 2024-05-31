# ETL Pipeline: A Billion Records to Postgres

## Overview

Welcome to the **ETL Pipeline** repository. This experimental project is designed to load a Apache Kafka stream of large dataset csv records to a JDBC sink (Postgres) via Apache Flink while applying minor transformations. It is built with Java 17, Gradle and Docker, aiming to provide a template code-base for ETL pipelines with large datasets via low latency streaming. This takes full advantage of local machine's resources and convenient for Proof-Of-Concept demos, learning, experimentation etc.

[//]: # ([Optional: Include a screenshot or a gif of the project])

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [Contact](#contact)

[//]: # (## Features)

[//]: # ()
[//]: # (- **Feature 1:** Detailed explanation of feature 1.)

[//]: # (- **Feature 2:** Detailed explanation of feature 2.)

[//]: # (- **Feature 3:** Detailed explanation of feature 3.)

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
    ./start-kafka-prodicer.sh
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

---

Thank you for checking out **ETL Pipeline**! We hope you find it useful and engaging. Happy coding!

[//]: # ([Optional: Include any acknowledgments or credits here])

---

[//]: # (*Note: Replace placeholders with actual information relevant to your project.*)