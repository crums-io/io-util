io-util
=======

A small, scalable Java library for slicing and dicing fixed width tables on disk, among other things.

## Objective

The objective is to provide reusable blocks of code for building efficient, custom binary data stores.

## What does it do?

Early days.

The highest level problem tackled so far (hot out of the oven and under development) concerns building, searching, and
maintaining a large, externally stored,
fixed width, sorted table. The idea is that the library user specifies the row width (in bytes), a row comparison
function (which implicitly defines any given row's key), and an optional delete codec, and they can spin up a
fairly efficient, CRUD-like, durable, custom table in a few lines of code. The implementation models a sorted map
data structure (Search for `karoon` in the code.)

But because the the parts that make up karoon can hopefully be easily rearranged (in code, of course) to tackle
other problems, I hope you'll find the parts are actually more interesting than solution itself. It's a bottom up,
compositional approach; definitely not a framework approach.

Here's an incomplete, but growing list of component features and attributes.

* Persistent, fail-safe counter (called a Keystone). Keystones can be persisted at any offset of a file, provide
  all-or-nothing update semantics, and are designed to tolerate abnormal program shutdown (e.g. power failure).
  
* A simple, fixed width table abstraction, coupled with a keystone to maintain row count. The abstraction doesn't
  divy up a row into columns. That's the user's business.
  
* Searching over persisted, sorted, fixed width tables. This models a sorted map and uses a user-defined row comparison
  function (called a RowOrder), or may be more like a relational table with a unique index on one of its columns.
  The "key" here is just a byte buffer as wide as the table's row width, with it's "index column" filled
  according to the search term.
  Sorted tables (SortedTable in the code) are write-once data structures. A builder (SortedTableBuilder) is provided
  to create small sorted tables. To update a table we need the next item.
  
* Searching over table stacks (called a TableSet). This presents a logical view of a single table over a stack of
  tables. The stack refers to the fact that if a matching row is found in a table
  at the top of the stack, then the tables lower in the stack are not consulted. That is, tables at the top of
  the stack can <em>override</em> tables further down the stack. In this way, it's possible to
  push new (write-once) tables on top of old ones. Updating existing rows (in the logical table) comes for free.
  But of course this doesn't scale: we need to merge the tables once in a while.
  
* Multi-way merge of ranked, sorted, fixed width tables. Table rank comes into play when one table's row "overrides"
  that of another--much, nay, exactly, like TableSets. That is both tables have a row with a same "key", but only one of the rows (the one from the higher
  ranking table) makes it to the merged output table. The merge algorithm is designed to minimize row comparisons
  and can often block transfer a region of a source table to a target table without ever having to load that region
  into memory. For a sketch of the algorithm see [here](https://github.com/gnahraf/io-util/blob/master/src/main/java/com/gnahraf/io/store/table/merge/BaseMergeSource.java).
  
* Deletes. Deletes are supported by updating a row with content indicating it's been tombstoned. How this encoding
  is done is left to the user defined DeleteCodec. In the worst case design, each row sets
  aside a byte for a deletion marker field. More typically, though, deletion markers can be worked directly into
  a row's content by using a domain-specifc value (e.g. -1 in a count field). Tombstoned rows are written to new tables.
  A version of the table stack (called TableSetD) recognizes these tombstoned rows--again, using the user-defined
  DeleteCodec, so that they are skipped. Likewise, support for DeleteCodec-aware, multi-way merging, is provided:
  the merge is aware of tables lower in the stack (the back set) that are not being merged, and tombstoned rows are
  eventually skipped (removed) in the merged output when there's no back set or when the back set does not contain the
  row.
  

## How to build

### Prerequisites

* JDK 1.7
* Maven 3.x
* Internet connection (possible dependency downloads by maven)

### Building

Standard maven build. Change to the io-util project directory and invoke maven..

<pre>
$ cd io-util
$ mvn clean package
</pre>

The tests take a while (under a minute, at the time authored). When maven finishes
you'll find a `target/test-artifacts` directory containing the side effects of the tests. If you want to also
include the few performance tests (adds another 2 minutes, as of this date) add the `-Dperf_test=true` switch:

`$ mvn clean package -Dperf_test=true`

To generate javadocs

`$ mvn javadoc:javadoc`

These get generated in the `target/site/apidocs` directory. Move that directory to a more
permanent location, if you don't want it wiped out on the next build.

(Btw, if anyone knows how to publish javadocs to github.com projects, please share how.)

### Early performance results

There's one write-heavy stress test for karoon. To run it

`$ mvn clean test -Dtest=TStoreBigTest`

This inserts 1M randomly generated 64-byte rows. Typical insertion rates are over 10,000 rows
per second. Note that the randomness actually works against the store: more real world, lumpy
data sets should perform better than what this test generates.

## Roadmap

Looking beyond the immediate TODOs (such as testing the table iterators, and lots more testing)
here are some things I'd like to work on next, with stuff at the top of the list tending to be higher
priority than those further down.


* Explore implementing secondary indexes in karoon.
* Hook karoon to netty.
* Performance / stress testing.
* Performance improvements.
  * The searcher can easily improve on disk I/O, by lazily caching certain rows, for example.
    (The existing tests indicate the file system does a pretty good job caching these itself, but of course,
    this varies by hardware, OS, and file system.)
* Support for annotating references to arbitrary length blobs in fixed width tables.

This roadmap addresses some of my own itches, and I hope some of yours too.

Enjoy! And remember, contributions, whether in the form of suggestions, ideas, code, or forks, are welcome.

Babak<br/>
Dec. 23, 2013
