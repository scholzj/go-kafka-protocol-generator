# Go Kafka Protocol Generator

A Java-based code generator that produces Go encode/decode code for **every Apache Kafka request and
response**. The protocol message definitions are read directly from the `kafka-clients` JAR's bundled
JSON resources (`common/message/*.json`), so there is no need to vendor the spec files. The generated
Go code is written into the [go-kafka-protocol](https://github.com/scholzj/go-kafka-protocol)
repository, which is included here as a git submodule.

## Layout

```
pom.xml, src/        the Java/Maven generator (run from the repo root)
go-kafka-protocol/   git submodule -> github.com/scholzj/go-kafka-protocol (the output target)
```

The Kafka version (and therefore the set of messages generated) is controlled by the
`kafka.version` property in `pom.xml` (currently 4.3.0).

## Getting the submodule

After cloning this repository, initialise the submodule:

```bash
git submodule update --init
```

## Building

```bash
mvn clean package
```

This creates a self-contained JAR in `target/go-kafka-generator-1.0.0-jar-with-dependencies.jar`
(the Kafka message JSONs are bundled into it).

## Usage

Run from the repository root. With no arguments it reads the message definitions from the bundled
`kafka-clients` JAR and writes into the `go-kafka-protocol/` submodule:

```bash
java -jar target/go-kafka-generator-1.0.0-jar-with-dependencies.jar
gofmt -w go-kafka-protocol/api go-kafka-protocol/apis
```

An optional single argument overrides the output directory:

```bash
java -jar target/go-kafka-generator-1.0.0-jar-with-dependencies.jar <output-directory>
# or via Maven
mvn exec:java -Dexec.mainClass="cz.scholz.generator.Generator" -Dexec.args="go-kafka-protocol/api"
```

The generated code updates the submodule's working tree; review it there and commit/push it to the
go-kafka-protocol repository separately.

### Arguments

- `output-directory` (optional): directory for the generated `api/` packages (default:
  `go-kafka-protocol/api`; `apis.go` is written to the sibling `apis/` directory)

## How It Works

1. Reads JSON specification files from the spec directory
2. Parses each JSON file (stripping JavaScript-style comments)
3. Generates Go code following the patterns in the existing manually-written code:
   - Package structure based on API name
   - Struct definitions with proper types
   - Write/Read methods for serialization
   - Encoder/decoder methods for nested types
   - Tagged fields support for flexible versions
   - PrettyPrint methods for debugging

## Generated Code Structure

For each JSON spec file, the generator creates:

- **Package**: Lowercase version of the API name (e.g., `ApiVersionsRequest` → `apiversions`)
- **File**: `request.go` or `response.go` based on the spec type
- **Main struct**: Named after the API (e.g., `ApiVersionsRequest`)
- **Nested structs**: For complex nested types (e.g., `ApiVersionsResponseApiKey`)
- **Methods**:
  - `Write()`: Serializes the struct to bytes
  - `Read()`: Deserializes bytes to struct
  - Encoder/decoder methods for nested array types
  - Tagged fields encoder/decoder (for flexible versions)
  - `PrettyPrint()`: Human-readable string representation

## Notes

- The generator matches the existing code patterns as closely as possible
- Some minor formatting differences may exist (spacing, variable names)
- The generator handles version-specific fields, nullable types, and flexible versions
- Tagged fields are supported for flexible protocol versions

