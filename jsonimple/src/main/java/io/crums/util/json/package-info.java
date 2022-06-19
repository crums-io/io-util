/*
 * Copyright 2021-2022 Babak Farhang
 */
/**
 * Utilities and abstractions for using the JSON model.
 * <p>
 * Jsonimple is a derivative of <a href="https://github.com/fangyidong/json-simple">simple-json</a>.
 * Like its parent, it aims to be simple and lightweight (no external dependencies). Another aim is
 * to simplify migraton from the parent to this one: other than adjusting imported packages, most
 * code should compile fine.
 * </p><p>
 * A few easy-to-fix warts in the parent library were fixed, and some type safety compiler warnings
 * were quashed. The abstractions you're probably already familiar are in the
 * {@linkplain io.crums.util.json.simple} package.
 * </p><p>
 * This package contains a few utilities and abstractions for working with the object model:
 * <ul>
 * <li>For indented output: {@linkplain io.crums.util.json.JsonPrinter}</li>
 * <li>For common validation steps used in parser implementations: {@linkplain io.crums.util.json.JsonUtils}</li>
 * <li>Parser interfaces: {@linkplain io.crums.util.json.JsonEntityWriter} and
 * {@linkplain io.crums.util.json.JsonEntityReader}. These come with useful default methods: a concrete type
 * usually needs to only implement one abstract method per interface.</li>
 * </ul>
 * </p>
 * 
 * @see io.crums.util.json.JsonEntityParser
 */
package io.crums.util.json;