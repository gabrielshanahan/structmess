# structmess

This project uses Quarkus with Kotlin, the Supersonic Subatomic Java Framework with Kotlin support.

## PostgreSQL Message Queue System

This project implements a high-performance message queue system using PostgreSQL with:

- SKIP LOCKED for efficient message claiming without blocking
- LISTEN/NOTIFY for real-time message notifications
- JSONB for flexible message payloads
- FIFO (First In, First Out) message processing
- Automatic retry mechanism

It follows a clean layered architecture:
- REST API layer (`/api`)
- Service layer (`/service`)
- Messaging infrastructure layer (`/messaging`)
- Repository pattern using direct JDBC (no Hibernate/ORM)
- Domain model (`/domain`)

### API Endpoints

- `POST /messages/{topic}` - Send a message to a topic
- `POST /messages/{topic}/claim` - Manually claim a message for processing
- `POST /messages/{id}/ack` - Acknowledge successful processing of a message
- `POST /messages/{id}/nack` - Mark a message as failed (with optional retry)

## Getting Started

### Prerequisites

- JDK 21 or later
- Docker and Docker Compose (for PostgreSQL)

### Setting up the Database

Start the PostgreSQL database using Docker Compose:

```shell script
docker-compose up -d
```

### Running the Application

```shell script
./gradlew quarkusDev
```

The application will be available at <http://localhost:8080>

## Message Queue Usage Examples

### Sending a Message

```http
POST /messages/example-topic HTTP/1.1
Content-Type: application/json

{
  "key": "value",
  "data": {
    "nested": "content"
  }
}
```

### Consuming Messages

You can consume messages programmatically:

```kotlin
// Using MessageService to process messages
messageService.processMessages("example-topic") { message ->
    // Process the message
    val payload = objectMapper.readValue(message.payload, Map::class.java)
    println("Processing message: $payload")
    
    // Return true for success, false for failure
    Uni.createFrom().item(true)
}.subscribe().with(
    { result -> println("Result: $result") },
    { error -> println("Error: $error") }
)
```

Or claim messages manually via the API:

```http
POST /messages/example-topic/claim HTTP/1.1
Content-Type: application/json
```

## Kotlin Support

This project uses Kotlin as the primary programming language. It's configured with:

- Kotlin JVM plugin
- Kotlin AllOpen plugin for Quarkus compatibility
- Quarkus Kotlin extension

Source code is organized in the standard Maven/Gradle structure with Kotlin sources in:
- `src/main/kotlin` for application code
- `src/test/kotlin` for test code

Java code can still be used alongside Kotlin if needed.

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./gradlew quarkusDev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./gradlew build
```

It produces the `quarkus-run.jar` file in the `build/quarkus-app/` directory.
Be aware that it's not an _über-jar_ as the dependencies are copied into the `build/quarkus-app/lib/` directory.

The application is now runnable using `java -jar build/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./gradlew build -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar build/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./gradlew build -Dquarkus.native.enabled=true
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./gradlew build -Dquarkus.native.enabled=true -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./build/structmess-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/gradle-tooling>.

## Related Guides

- REST ([guide](https://quarkus.io/guides/rest)): A Jakarta REST implementation utilizing build time processing and Vert.x. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it.
- Flyway ([guide](https://quarkus.io/guides/flyway)): Handle your database schema migrations
- REST Jackson ([guide](https://quarkus.io/guides/rest#json-serialisation)): Jackson serialization support for Quarkus REST. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it
- JDBC Driver - PostgreSQL ([guide](https://quarkus.io/guides/datasource)): Connect to the PostgreSQL database via JDBC

## Provided Code

### REST

Easily start your REST Web Services

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)
