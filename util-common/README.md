util-common
===========

The most widely imported general-purpose utilities module in this repo. Three families of
classes live here:

* **Collections** (`io.crums.util.collections`) - `Lists`, `Sets`, `Maps`, `CollectionUtils`,
`Iterators`, `CachingList`, `BigList`, `NumberList`, `PrimitiveComparator`, and the
type-equivalence helpers `ClassEquivalent`, `DelegateEquivalent`, `UnanonymousType`, `Isomorphism`.
* **Strings and ids** (`io.crums.util`) - `Strings`, `IntegralStrings`, `RandomId`, `BigShort`,
`TidyProperties`, `SuppliedValue`, `TaskStack`, `CacheWheel`, `CloseableIterator`.
* **NIO channels** (`io.crums.util.io.channels`) - `ChannelUtils`, `SharedChannel`.

## Notable Mentions

* [Lists](./src/main/java/io/crums/util/collections/Lists.java) - Efficient utility methods for
creating lazy, read-only views of arrays and other lists. Supports generic type transformations
and downcasts.

* [TaskStack](./src/main/java/io/crums/util/TaskStack.java) - Root releaser (closer) for a
try-with-resources clause. Is itself a `Closeable` and unwinds (pops) a stack of other
`Closeable` instances pushed onto it. Also supports releasing locks. Use when you need to
acquire and release more than one resource, or must acquire several resources but only if you
succeed in acquiring them all.

* [TidyProperties](./src/main/java/io/crums/util/TidyProperties.java) - Writes *tidy* properties
files by letting you define the order name/value pairs should appear, so related settings can be
grouped together.

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>util-common</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util`
* **Exports:** `io.crums.util`, `io.crums.util.collections`, `io.crums.util.io.channels`
* **Requires:** `java.base`, `io.crums.util.api` (transitive)
