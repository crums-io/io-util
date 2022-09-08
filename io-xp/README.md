io-xp
=====

Less common utilities. Notable mention:

* A small flexible fuzzy controller. One use is to throttle work (`FuzzyThrottler`) at the production end when the consuming end is having trouble keeping up. To maximize throughput, you often want to near this limit, but not cross it. This "limit" is often hard to know quantify (in units) or calculate apriori: instead we may know certain symptoms of too much work. For example, is the work backlog growing too fast.

## Maven

To use this module add this dependency in your POM file:


```
  <dependency>
    <groupId>io.crums</groupId>
    <artifactId>io-xp</artifactId>
    <version>1.0.0</version>
  </dependency>
```


## Module Dependencies

JPMS dependencies:
* `java.base`
* `io.crums.util`


