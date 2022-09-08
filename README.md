Overview 
========

A small set of Java utility modules for some general / specific tasks:

1. [io-util](./io-util/README.md) - I/O, buffer, and some common utilities. The `Lists` utility methods find a lot of use in other crums.io projects. *(No external dependencies.)*
2. [jsonimple](https://github.com/crums-io/io-util/tree/master/jsonimple) - json-simple knock off with some bug fixes, and a few added features. *(No external dependencies.)*
3. [io-xp](./io-xp/README.md) - Less used stuff. Includes a fuzzy controller.
4. [table-io](./table-io/README.md) - Simple, fast, fail-safe, fixed-width, external tables (on disk).

(To use these as a maven dependency, use the artifact coordinates listed in the links above.)

## Build (install)

This is a standard maven build.

### Prerequisites

* JDK 16+
* Maven 3.x
* Internet connection (possible dependency downloads by maven)

### Building & Installing

Clone this repo and then invoke

```
$ mvn clean install -DskipTests
```

Note running the tests requires a local install of this small [library](https://github.com/gnahraf/junit-io). 
(This will be fixed in the next version.) After running the tests you'll find a `target/test-outputs` directory containing the side effects of the tests.

You'll want to profile performance. To get a rudimentary idea for stuff like external merges, include the `perf_test` switch (takes about 2 minutes):

```
$ mvn clean install -Dperf_test=true`
```

## JPMS Ready

Module information is included in the build so that projects can load the library under the module system.
Its only dependency is the default module `java.base`.


## Milestones

Sept. 6, 2022: First version pushed to Maven Central

Feb. 11, 2021: Released under LGPL.

Jan. 7, 2014: Initial booha






