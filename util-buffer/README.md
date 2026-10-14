util-buffer
===========

`ByteBuffer`-oriented utilities (`io.crums.util.io.buffer`): fixed-width and named-field
partitioning of a backing buffer or file (`Partition`, `Partitioning`, `FilePartition`,
`NamedParts`), buffer-list views (`ReadOnlyBufferList`), a base class for fixed-width records
(`Block`), and buffer-copy/compare helpers (`BufferUtils`, `BufferOp`).

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>util-buffer</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util.io.buffer`
* **Exports:** `io.crums.util.io.buffer`
* **Requires:** `java.base`, `io.crums.util` (transitive)
