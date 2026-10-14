dir-traverse
============

A generic tree-traversal abstraction (`io.crums.util.tree`: `AbstractTraverser`,
`TraverseFrame`, `TraverseListener`) and its file-system implementation
(`io.crums.util.tree.fs`: `FileSystemTraverser`, `DirectoryOrdering`, `DirectoryRemover`) for
recursive directory walking, ordering, and removal.

## Maven

To use this module add this dependency in your POM file:

```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>dir-traverse</artifactId>
    <version>1.0.5-SNAPSHOT</version>
  </dependency>
```

## JPMS Module

* **Name:** `io.crums.util.tree`
* **Exports:** `io.crums.util.tree`, `io.crums.util.tree.fs`
* **Requires:** `java.base`, `io.crums.util` (transitive)
