/*
 * Copyright 2021 Babak Farhang
 */
/**
 * This objective was that this package not be married to any
 * one JSON parser implementation or outside dependencies. However, I bit the bullet and brought
 * in the <code>simple-json</code> since it had a few easy-to-fix warts. Once, I did that, it made
 * sense to bring in a few abstractions I've been using specific to <code>simple-json</code>.
 * 
 * @see https://github.com/fangyidong/json-simple
 * @see {@linkplain io.crums.util.json.simple}
 * @see io.crums.util.json.JsonUtils
 * @see io.crums.util.json.JsonPrinter
 * @see io.crums.util.json.JsonEntityWriter
 * @see io.crums.util.json.JsonEntityReader
 */
package io.crums.util.json;