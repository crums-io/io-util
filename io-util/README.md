io-util
=======

Common utility classes, some dealing with I/O.

## Maven

To use this module add this dependency in your POM file:


```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>io-util</artifactId>
    <version>1.0.0</version>
  </dependency>
```

## Notable Mentions

Most of the utilities and types defined in this module are actually a bit specialized.
I'll highlight a few, however, that find more general use.

* [Lists](https://github.com/crums-io/io-util/blob/master/io-util/src/main/java/io/crums/util/Lists.java) - Efficient utility
methods for creating lazy, read-only, views of arrays and other lists. Supports generic type transformations and downcasts.

* [TaskStack](https://github.com/crums-io/io-util/blob/master/io-util/src/main/java/io/crums/util/TaskStack.java) - Root releaaser
(closer) for try-with-resource clause. Is a `Closeable` and unwinds (pops) the stack by closing other `Closeable` instances pushed onto its stack. Also supports releasing locks. Use when you need to acquire and release more than one resource; or if you must acquire multiple resources, but only if you succeed in acquiring them all.

* [TidyProperties](https://github.com/crums-io/io-util/blob/master/io-util/src/main/java/io/crums/util/TidyProperties.java) - Writes *tidy* properties files by allowing you to define the order name/value pairs should appear. The idea is to group related settings near each other. It's supposed to be human readable after all.


## Module Dependencies

JPMS dependencies:
* `java.base`

