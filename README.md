Overview 
========

A small set of Java utility modules, factored to keep cross-dependencies minimal. Everything
lives under the `io.crums.util` namespace.

1. [util-api](./util-api/README.md) - Types meant to appear in the public API of other modules
(`Serial`, `Opening`, `Base64_32`, `Digest`, etc). *(No external dependencies.)*
2. [util-common](./util-common/README.md) - The most widely used module: collection utilities
(`Lists`, `Sets`, `Maps`, `CollectionUtils`, ...), general string/id utilities (`Strings`,
`TaskStack`, `TidyProperties`, ...), and NIO channel utilities (`ChannelUtils`, `SharedChannel`).
Depends on `util-api`.
3. [io-util](./io-util/README.md) - General file I/O utilities (`FileUtils`, `BackedupFile`,
`FileLineIterator`, `IoBridges`, `IoStateException`). Depends on `util-common`.
4. [util-buffer](./util-buffer/README.md) - `ByteBuffer` partitioning and buffer-copy/compare
helpers. Depends on `util-common`.
5. [dir-traverse](./dir-traverse/README.md) - A generic tree-traversal abstraction plus a
file-system traversal/ordering/removal implementation. Depends on `util-common`.
6. [alf](./alf/README.md) - Bit-packing utilities and a compressed, random-access bag of
ascending integers (simplified Elias-Fano encoding). Depends on `util-common`.
7. [io-xp](./io-xp/README.md) - Less used stuff. Includes a fuzzy controller. Depends on `util-common`.
8. [table-io](./table-io/README.md) - Simple, fast, fail-safe, fixed-width, external tables (on
disk). Multi-way external merge sort. Depends on `io-xp`, `util-buffer`, and `io-util`.
9. [jsonimple](./jsonimple/README.md) - json-simple knock off with some bug fixes, and a few added
features. *(No external dependencies.)*

(To use these as a maven dependency, use the artifact coordinates above, groupId `io.crums`,
under this reactor's current version.)

## Build (install)

This is a standard maven build.

### Prerequisites

* JDK 16+
* Maven 3.x
* Internet connection (possible dependency downloads by maven)

### Building

Clone this repo and then invoke

```
$ mvn clean install
```

Note after running the tests you'll find a `target/test-outputs` directory containing the side effects of the tests.

You'll want to profile performance. To get a rudimentary idea for stuff like external merges, include the `perf_test` switch (takes about 2 minutes):

```
$ mvn clean install -Dperf_test=true`
```

## JPMS Ready

Module information is included in the build so that projects can load these libraries under the
module system. `util-api` and `jsonimple` depend only on `java.base`; the other modules depend on
each other as described above.

Note a module's JPMS name doesn't always match its Maven artifactId. See each module's README for
its exact module name and exported packages; the non-obvious ones:

* `alf` -> `io.crums.util.io.sef`
* `dir-traverse` -> `io.crums.util.tree`
* `table-io` -> `io.crums.util.io.store`
* `io-xp` -> `io.crums.util.xp`
* `jsonimple` -> `io.crums.jsonimple`


## Milestones

Sept. 6, 2022: First version pushed to Maven Central

Feb. 11, 2021: Released under LGPL.

Jan. 7, 2014: Initial booha






