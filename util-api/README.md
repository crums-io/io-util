util-api
========

A small, stable vocabulary of types (`Opening`, `Serial`, `SerialFormatException`,
`FactoryException`, `Base64_32`, `Digest`, `Digests`) that tend to appear in the public
API signatures of other crums.io projects.

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>util-api</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util.api`
* **Exports:** `io.crums.util.api`
* **Requires:** `java.base`
