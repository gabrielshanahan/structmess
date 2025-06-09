# scoop Java/Quarkus Development Guidelines

This guide provides best practices for developing with Java, Kotlin, and Quarkus in the scoop project.

## Code Style and Structure

- Write clean, efficient, and well-documented Kotlin/Java code following Quarkus best practices
- Organize packages consistently:
   - `io.github.gabrielshanahan.scoop.api` - REST endpoints
   - `io.github.gabrielshanahan.scoop.configuration` - Application configuration
   - `io.github.gabrielshanahan.scoop.domain` - Domain models and business logic
   - `io.github.gabrielshanahan.scoop.messaging` - Message queue infrastructure
- Use descriptive method and variable names following camelCase convention
- Format code using ktfmt: `./gradlew ktfmtFormat`
- Run static analysis with detekt: `./gradlew detekt`

## Quarkus Specifics

- Use Quarkus Dev Mode for faster development: `./gradlew quarkusDev`
- Leverage hot reload capabilities during development
- Use Quarkus annotations effectively:
   - `@ApplicationScoped` for singleton components
   - `@Inject` for dependency injection
   - `@ConfigProperty` for configuration properties
- Utilize Quarkus Dev UI at http://localhost:8080/q/dev/ for development tools
- Always take care to discern where reactive and non-reactive approaches are used, and continue using that approach

## Naming Conventions

- Use PascalCase for class names (e.g., `PostgresMessageQueue`, `OperationRun`)
- Use camelCase for method and variable names (e.g., `processMessage`, `cooperationLineage`)
- Use ALL_CAPS for constants (e.g., `MAX_RETRY_ATTEMPTS`, `DEFAULT_TIMEOUT`)
- Name interfaces as a sentence beginning with the first-person singular pronoun "I", i.e. "IRetrieveMessages"
- Prefer a larger number of smaller classes with higher cohesion, as given by the name in the previous point, rather than a smaller number of larger classes with low cohesion 

## Java and Kotlin Usage

- Use Kotlin 2
- Leverage Kotlin features appropriately:
   - Extension functions for cleaner APIs
   - Data classes for DTOs and value objects
   - Null safety with nullable types and safe calls
   - Coroutines for asynchronous programming
- Utilize Quarkus BOM for dependency management

## Configuration and Properties

- Store configuration in `application.properties` or `application.yaml`
- Use `@ConfigProperty` for type-safe configuration injection
- Leverage Quarkus profiles (dev, test, prod) for environment-specific configurations
- Follow this hierarchy for configuration:
   1. Default values in code
   2. `application.properties`
   3. Environment variables
   4. Command-line arguments

## Dependency Injection and IoC

- Use CDI annotations (`@Inject`, `@ApplicationScoped`, etc.) for clean and testable code
- Prefer constructor injection over field injection for better testability
- Use qualifiers when multiple implementations of an interface exist
- Define clear component boundaries and responsibilities

## Testing

- Write tests with JUnit 5 and use `@QuarkusTest` for integration tests
- Use REST-assured for testing REST endpoints
- Implement test containers for integration testing with PostgreSQL
- Follow this testing pyramid:
   1. Unit tests for business logic
   2. Integration tests for component interactions
   3. End-to-end tests for critical paths
- Run tests with: `./gradlew test`

## Performance and Scalability

- Use reactive programming with Mutiny for non-blocking I/O
- Leverage Quarkus' reactive PostgreSQL client for database operations
- Implement proper connection pooling
- Use prepared statements for database queries
- Optimize SQL queries with appropriate indexes
- Consider using Quarkus' native compilation for production deployments

## Security

- Use parameterized queries to prevent SQL injection
- Secure sensitive configuration using Quarkus' config encryption
- Follow the principle of least privilege

## Logging and Monitoring

- Use the Quarkus logging system with appropriate log levels:
   - ERROR: For errors that need immediate attention
   - WARN: For potentially harmful situations
   - INFO: For general information about application flow
   - DEBUG: For detailed information useful for debugging
- Include contextual information in log messages (request ID, operation name)
- Implement health checks for monitoring application status
- Use structured logging where possible

## API Documentation

- Document all REST endpoints with OpenAPI annotations
- Include clear descriptions, parameter details, and response examples
- Generate and maintain up-to-date API documentation
- Follow RESTful API design principles:
   - Use appropriate HTTP methods (GET, POST, PUT, DELETE)
   - Return appropriate status codes
   - Use consistent URL patterns

## Data Access

- Use Flyway for database migrations
- Create clear, descriptive migration scripts
- Use the reactive PostgreSQL client for database operations
- Implement proper error handling for database operations
- Use transactions appropriately

## Build and Deployment

- Use Gradle with Quarkus plugins for building and packaging
- Configure multi-stage Docker builds for optimized container images
- Build options:
   - Standard JAR: `./gradlew build`
   - Über-JAR: `./gradlew build -Dquarkus.package.jar.type=uber-jar`
   - Native executable: `./gradlew build -Dquarkus.native.enabled=true`
- Use environment variables for configuration in different environments
- Implement proper health checks for container orchestration

## Structured Cooperation Protocol

- Follow the principles outlined in RFC.md when implementing protocol components
- Ensure proper handling of operation runs and their hierarchical relationships
- Implement proper error propagation and rollback mechanisms
- Maintain the cooperation lineage throughout the operation lifecycle