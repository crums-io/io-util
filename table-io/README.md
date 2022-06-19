table-io
========

A small, scalable Java library for slicing and dicing fixed width tables on disk, among other things.

## Objective

The goal is <em>not a framework</em>. Rather, it's a bottom up approach providing reusable blocks of code
for implementing efficient, custom binary data stores. Fail-safety (as in, for example, the plug was pulled before
an I/O operation completed) is a key requirement.


## Features

The highest level problem tackled so far concerns building, searching, and maintaining a large, externally stored,
fixed width, sorted table. The idea is that the library user specifies the row width (in bytes), a row comparison
function (which implicitly defines any given row's key), and an optional delete codec, and they can spin up a
fairly efficient, CRUD-like, durable, custom table in a few lines of code. This models a sorted map
data structure. An old package called *karoon* implemented this map, but it involved too many
"hard coded" design decisions, so I dropped it (still in the repo, of course).

Here's an incomplete, but growing list of component features and attributes.

* Persistent, fail-safe counter (called a Keystone). Keystones can be persisted at any offset of a file, provide
  all-or-nothing update semantics, and are designed to tolerate abnormal program shutdown (e.g. power failure).
  These in turn can be combined with other structures (e.g. next item) to lend them durable atomicity.
  
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
  into memory. For a sketch of the algorithm see [here](https://github.com/crums-io/io-util/blob/master/table-io/src/main/java/io/crums/io/store/table/merge/BaseMergeSource.java).
  
* Deletes. Deletes are supported by updating a row with content indicating it's been tombstoned. How this encoding
  is done is left to the user defined DeleteCodec. In the worst case design, each row sets
  aside a byte for a deletion marker field. More typically, though, deletion markers can be worked directly into
  a row's content by using a domain-specifc value (e.g. -1 in a count field). Tombstoned rows are written to new tables.
  A version of the table stack (called TableSetD) recognizes these tombstoned rows--again, using the user-defined
  DeleteCodec, so that they are skipped. Likewise, support for DeleteCodec-aware, multi-way merging, is provided:
  the merge is aware of tables lower in the stack (the back set) that are not being merged, and tombstoned rows are
  eventually skipped (removed) in the merged output when there's no back set or when the back set does not contain the
  row.

## Performance Tests

Use the perf_test switch:

> mvn clean test -Dperf_test=true

This shows statistics for some I/O bound operations, such as row merge rates under multi way external (on disk) sorting.



## Module Dependencies

JPMS dependencies:
* `java.base`
* `io.crums.util`
* `io.crums.util.xp`

