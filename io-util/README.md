io-util
=======

General-purpose file I/O utilities (`io.crums.util.io`): `FileUtils`, `BackedupFile`,
`FileLineIterator`, `IoBridges`, `IoStateException`.

Also (temporarily) home to `io.crums.util.main`, a small set of CLI-parsing helpers
(`ArgList`, `MainTemplate`, `TablePrint`, ...) slated for removal once its last few call-sites
migrate to picocli.

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>io-util</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util.io`
* **Exports:** `io.crums.util.io`, `io.crums.util.main`
* **Requires:** `java.base`, `io.crums.util` (transitive)
