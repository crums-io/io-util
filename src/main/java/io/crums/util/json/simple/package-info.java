/*
 * Copyright 2021 Babak Farhang
 */
/**
 * Lifted from <a href="https://github.com/fangyidong/json-simple">simple-json</a>.
 * A nice library (only 24k compiled) that is unfortunately not being maintained.
 * 
 * <h3>API Compatibility With simple-json</h3>
 * <p>
 * Method names and signatures have not been changed. The objective is that updating
 * code using <code>simple-json</code> to use this library will only need update the
 * <code>import</code> statements.
 * </p>
 * 
 * <h3>Changes</h3>
 * <p>
 * Logical or syntactic changes are tracked here. Source code changes are kept to
 * a minimum, so that diff's with the original are meaningful.
 * </p>
 * <ol>
 * <li>{@linkplain io.crums.util.json.simple.JSONValue#escape(String) JSONValue.escape(String)}:<br/> 
 * Fix for issue #8 at https://code.google.com/archive/p/json-simple/issues/8</li>
 * <li>Type-safety fixes:<br/>
 * Minmize compile-time naggings when building or using the library.</li>
 * <li>Insertion-ordered JSONObject Mappings:<br/>
 * {@linkplain io.crums.util.json.simple.JSONObject JSONObject} by default now
 * prints mappings in insertion order. This is usually not significantly slower
 * than the original unordered version. To create an old-style version, use the
 * {@linkplain io.crums.util.json.simple.JSONObject#newFastInstance() JSONObject.newFastInstance()}
 * pseudo-constructor.</li>
 * <li>StringBuffer -> StringBuilder:<br/>
 * Since instances are not safe under concurrent access anyway, <code>StringBuffer</code>s
 * were converted to <code>StringBuilder</code>s. This involved one API change:
 * {@linkplain io.crums.util.json.simple.JSONValue#escape(String, StringBuilder) JSONValue.escape(String, StringBuilder)}</li>
 * </ol>
 */
package io.crums.util.json.simple;