Jsonimple
=========

A knock off of the [json-simple](https://github.com/fangyidong/json-simple) parser.
Like its parent, it aims to be simple and lightweight (no external dependencies).
Another aim is to simplify migrating code from the parent to this one.

## API Compatibility With simple-json

The base API for stuff like `JSONObject`, `JSONArray` has not changed. For most uses of the
old library, the only changes required in source files should be in their import statements.

### Changes

Logical or syntactic changes are tracked here. Source code changes have been kept to a minimum,
so that diffs from the orginal can be meaningful. Here's a summary:

1. `JSONValue.escape(String)` - Fix for parent [issue 8](https://code.google.com/archive/p/json-simple/issues/8)
2. Type-safety. Fix to minmize compile-time naggings when building or using the library.
2. Ordered `JSONObject` fields. By default, fields are written out in the order they were added. This uses a
`LinkedHashMap`. The pseudo constructor `JSONObject.newFastInstance()` falls back to the more efficient
(but jumbled order) `HashMap` implementation. The parser uses this more efficient version of JSONObject on the read-path.
2. `StringBuilder` in lieux of `StringBuffer`. This necessitated one hopefully low impact *API change*, namely
`JSONValue.escape(String, StringBuilder)`.

## Other Features

A few simple features and abstractions are included to aid working with the object model:

* *Indented JSON*. `io.crums.util.json.JsonPrinter` supports pretty printing.
* *Boilerplate validation*. `io.crums.util.json.JsonUtils` for common validation steps when reading JSON.
* *Parser implementation interfaces*. The `JsonEntityWriter` and `JsonEntityReader` interfaces define useful default methods for 
serializing and de-serializing Objects to and from JSON. Concrete parser implementations usually implement both and need only implement 2 abstract methods, one for each interface.

## Module Dependencies

JPMS dependencies:
* `java.base`

