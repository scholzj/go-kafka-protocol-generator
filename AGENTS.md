# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## What this project is

A **Java code generator** that reads Apache Kafka protocol message
specifications (the JSON files Kafka itself uses) and emits **Go source code**
for encoding (`Write`) and decoding (`Read`) Kafka requests and responses.

The Go output is part of a separate Go module, `github.com/scholzj/go-kafka-protocol`,
whose hand-written runtime (the `protocol` package) the generated code depends on.

> ✅ **Status: working.** `mvn clean package` (from the repo root), then running the JAR (which reads
> the Kafka message JSONs from the bundled kafka-clients JAR) into the `go-kafka-protocol/` submodule,
> produces Go for **all ~178 requests/responses** that builds, vets, and passes the tests
> (`cd go-kafka-protocol && go build ./... && go vet ./... && go test ./...`).
> The generated output is then `gofmt`'d. See [Regenerating](#regenerating).
>
> The generator's intended output is the hand-written code in the upstream repo
> **https://github.com/scholzj/go-kafka-protocol** (clone it for reference — it is
> the gold standard the generator reproduces). The generator improves on it in a
> few deliberate ways: it uses the nullable-aware `protocol` helpers everywhere a
> field/array is nullable (the hand-written code dereferences some nullable values
> and would nil-panic), it uses the receiver's `ApiVersion` consistently, it calls
> the request/response-appropriate `isXFlexible` helper, and it drops a duplicated
> decoder assignment present in the hand-written `produce` code.

## Repository layout

The Java/Maven generator lives at the repo root; the generated Go code lives in the
**`go-kafka-protocol` git submodule** (the real github.com/scholzj/go-kafka-protocol repo).

```
pom.xml             Maven build (Gson, kafka-clients ${kafka.version}, Java 11, assembly plugin)
README.md           user-facing build/run instructions
.gitignore          ignores /target/ and IDE files
src/main/java/cz/scholz/generator/
  Generator.java          main(); reads message JSONs from the kafka-clients JAR, drives generation
  GoCodeGenerator.java    THE core — turns one ApiSpec into one .go file (~700 lines)
  GoTestGenerator.java    emits a round-trip <type>_test.go next to each message
  ApisGenerator.java      generates apis/apis.go (header-version lookup tables)
  model/
    ApiSpec.java          POJO for a spec file (apiKey, type, name, validVersions,
                          flexibleVersions, fields, commonStructs)
    Field.java            POJO for a field (+ deep copy() for inlining commonStructs)
  util/
    TypeUtils.java        Kafka type -> Go type; protocol Read/Write method name;
                          package/struct/camelCase naming (incl. nestedStructName)
    VersionUtils.java     parse version strings ("0+", "3", "0-4") -> start/end ints
    CommonStructResolver.java  inlines top-level commonStructs into the fields that reference them
    JsonCommentStripper.java   strips // and /* */ comments from the spec JSON
target/             Maven build output (gitignored; holds the runnable JAR)

INPUT: the Kafka protocol message definitions are NOT vendored here — they are read straight from
the kafka-clients JAR's resources (common/message/*.json). The Kafka version is the pom's
`kafka.version` property. All requests and responses are generated (validVersions: "none" and the
header/record "data" definitions are skipped).

go-kafka-protocol/  GIT SUBMODULE -> github.com/scholzj/go-kafka-protocol (the OUTPUT target)
  go.mod            module github.com/scholzj/go-kafka-protocol + google/uuid; go 1.23.5
  protocol/         HAND-WRITTEN runtime the generated code calls into (do not generate)
    protocol.go         Request/Response/headers, ReadRequest/Write, ReadResponse/Write
    types.go            Read*/Write* primitives + generic array helpers
    tagged_fields.go    TaggedField type + raw/length-bounded tagged-field read/write
    *_test.go
  apis/apis.go      GENERATED: RequestHeaderVersion / ResponseHeaderVersion lookup
  apis/apis_test.go HAND-WRITTEN: header-version table test
  api/<name>/       GENERATED: one package per API (~89 packages, all Kafka request/response),
                    request.go and/or response.go, e.g. api/apiversions/response.go
                    GENERATED: request_test.go / response_test.go (round-trip tests)
                    HAND-WRITTEN extras: apiversions/wire_golden_test.go,
                    apiversions/tagged_fields_test.go, metadata/nullable_roundtrip_test.go
```

> The submodule is pinned at an upstream commit; the generator updates its **working tree**. Those
> changes are committed and pushed to the go-kafka-protocol repo separately (this repo only records
> the submodule's commit, not the pending generated changes). After cloning, run
> `git submodule update --init` to populate it.

## Build & run

Requires a JDK (project targets Java 11; a newer JDK like 21 also builds) and Maven. Run everything
from the repo root.

```bash
git submodule update --init     # once, to populate go-kafka-protocol/
mvn clean package               # -> target/go-kafka-generator-1.0.0-jar-with-dependencies.jar
```

### Regenerating

With no arguments the generator reads the Kafka message JSONs from the bundled kafka-clients JAR and
writes into the submodule:

```bash
java -jar target/go-kafka-generator-1.0.0-jar-with-dependencies.jar
gofmt -w go-kafka-protocol/api go-kafka-protocol/apis
(cd go-kafka-protocol && go build ./... && go vet ./... && go test ./...)
```

`Generator` writes `<outputDir>/<package>/{request,response}.go` (plus a matching
`{request,response}_test.go`) for each spec and `<outputDir>/../apis/apis.go` (note: `apis.go`
lands in the *sibling* of `outputDir`). The generator emits tab-indented Go but does **not**
align trailing comments or sort imports — `gofmt` does that, so always gofmt the output. The
default output dir is `go-kafka-protocol/api`; override it with one positional arg. Per-message
generation is isolated (a failing message is reported and skipped, not fatal); the run prints a
`Generated N of M` summary.

### Tests & coverage (~70% of statements over ~89 generated packages)

Two layers, by design:
- **Generated round-trip tests** (`GoTestGenerator` → `request_test.go`/`response_test.go`):
  build one fully-populated message and, for every version in its valid range, do
  write → read → write and assert the two encodings are byte-identical, then call `PrettyPrint`.
  Comparing re-encoded bytes (not structs) sidesteps nil-vs-empty `rawTaggedFields` issues. These
  catch **asymmetric** encode/decode bugs and regenerate automatically; they do **not** prove the
  bytes match a real broker, and they don't exercise nil/unknown-tag/error paths (so api packages
  cap around ~75%).
- **Hand-written tests** (never regenerated): `wire_golden_test.go` pins actual bytes (the only
  thing that catches a *symmetric* bug like the records length encoding); `tagged_fields_test.go`
  and the protocol `*_forward_compat_test.go` cover unknown-tag / length-bounding paths;
  `nullable_roundtrip_test.go` covers nil and the encode-side null validation; `apis_test.go`
  covers the header tables. The hand-written runtime in `protocol/` sits at ~81% (the rest are
  `if err != nil` branches needing fault injection).

## How generation works (the mental model)

1. `Generator.main` reads every `common/message/*.json` resource from the kafka-clients JAR, strips
   comments (`JsonCommentStripper`), parses each with Gson into an `ApiSpec`, keeps only the
   `request`/`response` types (skips `header`/`data` and `validVersions: "none"`), and inlines any
   `commonStructs` (`CommonStructResolver`) so the rest of the generator sees plain inline structs.
2. For every spec it constructs a `GoCodeGenerator` and calls `generate()`, which
   appends Go to an internal `StringBuilder` via `line(indent, text)` / `blank()`
   helpers. **All Go code is produced from plain string literals** — no template
   engine. The class is structured as one method per output construct and is
   written to be read top-to-bottom; the class javadoc summarises the emit order.
3. `ApisGenerator` builds the two header-version switch functions from all specs.

`GoCodeGenerator.generate()` emits, in order: package + imports → main struct +
nested structs → `isRequestFlexible`/`isResponseFlexible` (flexible specs only) →
main `Write` → main `Read` → per-nested-object `<field>Encoder`/`<field>Decoder`
(+ `taggedFieldsEncoder<Field>`/`taggedFieldsDecoder<Field>` when that object has
its own tagged fields) → top-level `taggedFieldsEncoder`/`taggedFieldsDecoder`
(tagged specs only) → `PrettyPrint` methods.

### Key conventions encoded in the generator
- **Receiver name**: `req` for requests, `res` for responses. The receiver's
  `ApiVersion` is used for *all* version/flexible checks (Read included), for
  consistency.
- **Package name**: `ApiVersionsResponse` → `apiversions` (strip `Request`/`Response`,
  lowercase) — `TypeUtils.toPackageName`.
- **Nested struct type name** (`nestedStructName`): `parentStruct + singularised
  field name`, applied recursively — e.g. `Partitions` inside `MetadataResponseTopic`
  → `MetadataResponseTopicPartition`. The Go type comes from this path, *not* from the
  JSON `type`. Singularisation is just "drop a trailing `s`".
- **Encoder/decoder method names** (`methodName` / `assignMethodBaseNames`): `lcfirst(fieldName)
  + Encoder|Decoder`, e.g. `partitionsEncoder`. They are always methods on the **main** struct
  (so the receiver can reach `ApiVersion`); the nested value is passed as a `value` parameter.
  Names are assigned by a pre-pass that guarantees uniqueness: if two struct fields in different
  places share a name, the later one falls back to the path-derived nested struct name (e.g.
  `produceResponseResponsePartitionResponseEncoder`) so methods never collide. The
  `taggedFieldsEncoder<Base>` names use the same base.
- **Local variable names** (`safeVar`): a field's read/loop var is its lower-cased name, suffixed
  with `Val` if that would collide with a Go keyword (so a field named e.g. `Type` doesn't emit
  `type, err := …`).
- **Go types** (`goType`): struct field → `*[]Nested` (array) or `*Nested` (single);
  otherwise `TypeUtils.toGoType` — `string` → `*string`, `records`/`bytes` → `*[]byte`,
  `uuid` → `uuid.UUID`, `int8/16/32/64`, `uint16/32`, `float64`, `bool` by value, primitive
  array → `*[]elem`. (Note the runtime's casing quirk: `WriteUint16` but `ReadUInt16`.)
- **commonStructs**: resolved away before generation — `CommonStructResolver` deep-copies a
  shared struct's fields into every field that references it, so each occurrence becomes an
  ordinary inline nested struct (the path-derived naming then keeps every copy unique).
- **Versions** (`versionCondition`): absolute — `>= N` when start>0, `<= M` when end is
  bounded, `== N` for a single version; no guard for `0+`. Guards are emitted even when
  `validVersions` starts above 0 (matches the reference).
- **Flexible versions / tagged fields**: a spec is "flexible" when `flexibleVersions`
  start `< MAX` (most messages are; some old ones aren't). Flexible versions use the *compact* encodings
  and append a tagged-fields section. A field with a `tag` is written by the
  taggedFields encoder/decoder; a *nested* tagged field that also has regular versions is
  additionally written inline guarded by `if !isXFlexible(...)` (for the older
  non-flexible versions). Unknown tags round-trip through each struct's `rawTaggedFields`.
- **Nullability**: a field is nullable iff it has `nullableVersions`. Nullable
  strings/bytes/arrays/records use the `Nullable*` `protocol` helpers (pointer passed
  through, nil-safe); non-nullable ones dereference. This is the main place the generator
  improves on the hand-written reference, which dereferences some nullable values
  unconditionally. The struct field comments carry `(versions: …, nullable: …)` so a caller
  can see, per field, when null is permitted.
- **Encode-side null validation** (`emitNullCheck` / `nonNullableVersionPredicate`): every
  pointer field gets a guard at the top of its write that returns
  `fmt.Errorf("<Struct>.<Field> must not be nil in version %d", …)` when it is nil in a
  version where it is non-nullable. The version predicate is computed from `versions` ∖
  `nullableVersions`: no guard when the field is nullable across its whole life; an
  unconditional `if x == nil` when it is never nullable (this also removes the old nil-deref
  panic); otherwise a version-bounded check, e.g. `if req.ApiVersion < 8 && req.Foo == nil`.
  Validation is **encode-only** — `Read` stays lenient toward a peer's bytes. Value types
  (ints/bool/uuid) are never checked. Round-trip + validation behaviour is covered by
  `go-kafka-protocol/api/metadata/nullable_roundtrip_test.go`.

### The runtime API the generated code targets (`go-kafka-protocol/protocol`)
- Primitives: `WriteInt8/16/32/64`, `ReadInt8/16/32/64`, `WriteBool`/`ReadBool`,
  `WriteUUID`/`ReadUUID`, `WriteFloat64`, `WriteUint16/Uint32` /`ReadUInt16/UInt32`
  (note the inconsistent casing), varint/varlong/uvarint helpers.
- Strings/bytes/records: `Write/Read[Nullable][Compact]String|Bytes|Records`.
- Arrays (Go generics): `WriteArray[T]`, `ReadArray[T]`, `WriteNullableArray[T]`,
  `ReadNullableArray[T]`, `WriteNullableCompactArray[T]`, `ReadNullableCompactArray[T]`,
  taking an `ArrayEncoder[T]`/`ArrayReaderDecoder[T]` callback.
- Tagged fields: `TaggedField{Tag, Field}`, `WriteRawTaggedFields`,
  `ReadRawTaggedField(s)`, `ReadTaggedFields(r, decoderFunc)`.
- `TypeUtils.getProtocolWriteMethod` / `getProtocolReadMethod` map
  `(kafkaType, nullable, flexible)` → the helper name. They return `null` for
  struct types (which use a generated `<field>Encoder`/`Decoder` instead).

## Working tips for agents

- **Generated vs hand-written**: never edit `go-kafka-protocol/api/*` or `apis/apis.go` to
  "fix a bug" — fix the *generator* and regenerate (+ gofmt). Conversely,
  `go-kafka-protocol/protocol/*` is the hand-written runtime; change it there. The generated Go
  lives in the submodule, so its changes are committed/pushed to the go-kafka-protocol repo, not
  to this one (this repo only pins the submodule commit).
- The fastest debug loop is: edit Java → `mvn -q package` → run the JAR (defaults write into the
  submodule) → `gofmt -w` → `(cd go-kafka-protocol && go build ./...)` → read the errors.
  Pass an alternate output dir if you don't want to touch the submodule's working tree.
- The ground-truth reference is the upstream repo
  **https://github.com/scholzj/go-kafka-protocol** (also the submodule's pinned commit). Use
  `git -C go-kafka-protocol diff` to see what the generator changed vs upstream. Expect only the
  deliberate improvements (nullable helpers + encode-side null validation, per-version records
  encoding, receiver-based `ApiVersion`, `isResponseFlexible` in responses, `:=` instead of
  `var err error`, collision-proof method names, the de-duplicated `produce` decoder assignment,
  length-bounded tagged fields).
- `ApiVersionsResponse` (in the kafka-clients JAR, or view upstream) is the best small worked
  example (arrays, nested structs, tagged fields, flexible versions). `produce`/`fetch` exercise
  nested tagged fields, single nested structs, `records`, and `uuid`; messages with `commonStructs`
  (e.g. `OffsetCommit`, `JoinGroup`) exercise the inlining path.
- Gotchas baked into the design: tagged fields only exist in flexible versions (always
  emitted with compact helpers); the decoder var name is the nested struct name fully
  lowercased; `string`/`bytes`/`records`/array all pick compact vs non-compact with a
  runtime `isXFlexible(...)` check (so the non-flexible versions of a flexible message
  encode correctly); `validVersions` always starts at 0 in practice but several specs use a
  non-zero *floor* like `3-13` while field guards stay absolute.
- **Tagged-field length safety**: the runtime `protocol.ReadTaggedFields` reads each tag's
  declared length and hands the typed decoder a reader bounded to exactly those bytes, so unknown
  trailing bytes (forward-compat additions by a newer peer) can't desync the stream. The generated
  decoder's `default` case captures an unknown tag's raw bytes via `io.ReadAll(r)` and stores them
  in `rawTaggedFields`. Covered by `protocol/tagged_fields_forward_compat_test.go` and
  `api/apiversions/tagged_fields_test.go`.
- `Read` takes a `*protocol.Request`/`*protocol.Response` (pointer, no copy) and returns an error
  early if it or its `Body` is nil instead of panicking. Trailing bytes after a successful decode
  are intentionally tolerated (forward compatibility), not treated as an error.
- `PrettyPrint` renders `records`/`bytes` (`*[]byte`) payloads as a `<N bytes>` length placeholder,
  since the raw bytes aren't human-readable.
- `target/` (compiled classes + JARs) is committed; regenerate rather than hand-edit.
