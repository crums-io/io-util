alf
===

A compressed, random-access bag of ascending integers using simplified Elias-Fano encoding
(`io.crums.util.io.sef`: `Alf`, `AscLongs`, `AscBitsHistogram`), plus the bit-packing utilities
(`io.crums.util.io.bp`: `BitBuffer`, `Bits`) it's built on.

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>alf</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util.io.sef`
* **Exports:** `io.crums.util.io.bp`, `io.crums.util.io.sef`
* **Requires:** `java.base`, `io.crums.util` (transitive)
