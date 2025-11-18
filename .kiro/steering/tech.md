# Technology Stack

## Build System

- **Maven**: Multi-module Maven project
- **Java Version**: Java 11 (minimum)
- **Encoding**: UTF-8

## Key Dependencies

- **Google Guava**: Utility libraries
- **Google Guice**: Dependency injection framework
- **Apache Commons Lang3**: Common utilities
- **Joda-Time**: Date/time handling
- **Commons Logging**: Logging facade

## Testing

- **JUnit 4**: Primary testing framework
- **JUnitParams**: Parameterized tests
- **Mockito**: Mocking framework
- **Hamcrest**: Matcher library for assertions
- **H2**: In-memory database for testing

## Code Quality Tools

- **Checkstyle**: Code style enforcement (config: `etc/checkstyle.xml`)
- **SpotBugs**: Static analysis (exclusions: `etc/spotbugs-exclude.xml`)
- **JaCoCo**: Code coverage
- **SonarCloud**: Quality metrics

## Common Commands

### Build and Test
```bash
# Clean build with tests
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Run tests only
mvn test

# Run tests with specific thread count
mvn test -Dtest.threadCount=4
```

### Code Quality
```bash
# Run checkstyle
mvn checkstyle:check

# Run SpotBugs
mvn spotbugs:check

# Generate coverage report
mvn clean install -Pcoverage
```

### Release
```bash
# Prepare release
mvn release:prepare

# Perform release
mvn release:perform

# Release with signing (for Maven Central)
mvn clean install -Prelease
```

## Maven Profiles

- **coverage**: Enables JaCoCo code coverage
- **release**: Enables GPG signing and Maven Central publishing

## Test Configuration

- Tests run in parallel by default (4 threads)
- Test thread count can be overridden with `-Dtest.threadCount=N`
- Log4j configuration: `etc/log4j-maven.properties`
