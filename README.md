# Web-scale Data Management Project - Group 11

This project is a web-scale data management system designed to handle effectively transactions. It comprises microservices for order processing, payment handling, stock management, and ID generation, orchestrated using Docker. The system's architecture is based on a Choreography-based saga combined with RedLock for Redis to ensure consistency and logs for fault tolerance.

## Project Structure

- `env`: Folder containing the Redis environment variables for the Docker Compose deployment.
- `ids`: Folder containing the ID application logic and Dockerfile.
- `order`: Folder containing the order application logic and Dockerfile.
- `payment`: Folder containing the payment application logic and Dockerfile.
- `stock`: Folder containing the stock application logic and Dockerfile.
- `test`: Folder containing some basic correctness tests for the entire system. Feel free to enhance them.

## System Architecture

The system employs a Choreography-based saga pattern to manage distributed transactions across microservices. In this pattern, each local transaction publishes domain events that trigger local transactions in other services, ensuring eventual consistency without a central coordinator. However, to handle dirty reads and ensure stronger consistency, we use RedLock for Redis.

### Choreography-based Saga

- **ID Service**: Generates unique idempotent identifiers for logs.
- **Order Service**: Handles order creation, updates, and checkouts.
- **Payment Service**: Processes users and payments associated with orders.
- **Stock Service**: Manages stock levels and updates.

Each service communicates by emitting and listening to domain events, ensuring that all services are eventually consistent.

### Consistency and ault Tolerance

#### RedLock for Redis

To address the issue of dirty reads and ensure consistency across distributed transactions, the system utilizes RedLock for Redis. RedLock is a distributed lock management system that provides a way to implement distributed locks with Redis, ensuring that only one instance of a service can modify a resource at a time.

#### Log-based Fault Tolerance

To further ensure eventual consistency and handle faults, the system uses a log-based mechanism:

- **Log Entries**: Each action performed by a service is logged.
- **Log Parser**: A log parser periodically (every startup & 5 minutes) reviews the logs to detect and rectify inconsistencies.
- **Fault Correction**: The log parser can replay logs and perform compensating actions to fix inconsistencies.

### Log Parser

The log parser plays a crucial role in maintaining system reliability by:

- Periodically reviewing the log entries to identify incomplete or inconsistent transactions.
- Initiating compensating actions to correct any detected faults, ensuring that the system reaches an eventually consistent state.
- Ensuring that any partial transactions are either completed or rolled back appropriately.

## Prerequisites

- Docker
- Docker Compose

## Installation

1. **Install Docker**: Follow the instructions on the [Docker website](https://docs.docker.com/get-docker/).

2. **Install Docker Compose**: Follow the instructions on the [Docker Compose website](https://docs.docker.com/compose/install/).

## Deployment

### Local Deployment with Docker Compose

1. Clone the repository:

    ```sh
    git clone https://github.com/SebastiaanBeekman/WDM-Project.git
    cd WDM-Project
    ```

2. Build and run the services:

    ```sh
    docker-compose up --build
    ```

3. Run the tests (optional):

    ```sh
    cd test
    # Adjust tests as needed
    pytest
    ```

## Usage

### REST API Endpoints

#### ID Service

- **Generate ID**

    ```sh
    GET /ids/create
    ```

#### Order Service

- **Create Order**

    ```sh
    POST /orders/create/{user_id}
    ```

- **Find Order**

    ```sh
    GET /orders/find/{order_id}
    ```

- **Add Item**

    ```sh
    POST /orders/addItem/{order_id}/{item_id}/{quantity}
    ```

- **Checkout**

    ```sh
    POST /orders/checkout/{order_id}
    ```

#### Payment Service

- **Create User**

    ```sh
    POST /payment/create_user
    ```

- **Find User**

    ```sh
    GET /payment/find_user/{user_id}
    ```

- **Add credit**

    ```sh
    POST /payment/add_funds/{user_id}/{amount}
    ```

- **Remove credit**

    ```sh
    POST /payment/pay/{user_id}/{amount}
    ```

#### Stock Service

- **Create Item**

    ```sh
    POST /stock/item/create/{price}
    ```

- **Find Item**

    ```sh
    GET /stock/find/{item_id}
    ```

- **Add Stock**

    ```sh
    POST /stock/add/{item_id}/{amount}
    ```

- **Remove Stock**

    ```sh
    POST /stock/subtract/{item_id}/{amount}
    ```

### Example Requests

- **Create Order**

    ```sh
    curl -X POST http://127.0.0.1:8000/stock/item/create/3
    ```

## Tests

The project includes a set of basic correctness tests to ensure the functionality and reliability of the entire system. These tests are located in the `test` folder and can be run using `pytest`. Each test makes use of the logs which are created to track the state of the system. By iteratively creating logs and verifying the behaviour of the system, each microservice can be simulated and testes properly.

### Test Overview

- **Order Service Tests**: Validate order creation, retrieval, and updates.
- **Payment Service Tests**: Ensure the user and payment processing logic works correctly.
- **Stock Service Tests**: Check stock creation, addition, and retrieval functionalities.

### Running the Tests

To run the tests, navigate to the `test` folder and execute:

```sh
pytest
```

## Contributions

- Zoya van Meel:
  - Ensured fast log id uniqueness
  - Contributed to fault tolerance testing
  - Optimised the final product
  - Supported the group where needed
- Nina Oosterlaar:
  - Created code for the logs
  - Created the ids microservice
  - Contributed to fault tolerance testing
  - Optimised the final product
  - Supported the group where needed
- Sebastiaan Beekman:
  - Worked on logging
  - Developed fault tolerance
  - Implemented RedLock
  - Wrote most of the tests
  - Refactored the codebase (e.g. comments, extra abort checks, structuring)
  - Wrote the README
  - Supported the group where needed

We all contributed equally to the architecture and design of the code.
