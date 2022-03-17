/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.json;


/**
 * Pattern for both parsing and generating JSON.
 * 
 * @param <T> the entity type
 * @see JsonEntityWriter
 * @see JsonEntityReader
 */
public interface JsonEntityParser<T> extends JsonEntityWriter<T>, JsonEntityReader<T> {
  

}
