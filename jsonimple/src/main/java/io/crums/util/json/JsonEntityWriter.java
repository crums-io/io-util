/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json;

import java.util.List;

import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * The JSON entity output interface. Its only non-default method is
 * {@linkplain #injectEntity(Object, JSONObject)}.
 * 
 * @param <T> the entity type
 * 
 * @see #injectEntity(Object, JSONObject)
 * @see #toJsonObject(Object)
 * @see #toJsonArray(List)
 */
public interface JsonEntityWriter<T> {
  
  
  /**
   * Returns the given {@code entity} as JSON.
   * 
   * @return {@code injectEntity(entity, new JSONObject())}
   * @see #injectEntity(Object, JSONObject)
   */
  default JSONObject toJsonObject(T entity) {
    return injectEntity(entity, new JSONObject());
  }
  
  
  /**
   * Injects the given {@code entity}'s fields into the
   * given {@code JSONObject}. The rationale behind this method
   * is to support a kind of JSON polymorphism, wherein a higher
   * level object encodes its constituent parts in flattened form
   * (instead of encoding each constituent part as another separate,
   * named JSON object).
   * 
   * @param entity  not null
   * @param jObj    not null
   * 
   * @return the given {@code jObj}
   * @see #toJsonObject(Object)
   */
  JSONObject injectEntity(T entity, JSONObject jObj);
  
  
  /**
   * Returns the given list as a JSON array.
   * 
   * @param list
   * @return
   */
  default JSONArray toJsonArray(List<T> list) {
    JSONArray jArray = new JSONArray(list.size());
    list.forEach(t -> jArray.add(toJsonObject(t)));
    return jArray;
  }

}
