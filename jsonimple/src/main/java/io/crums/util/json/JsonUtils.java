/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json;

import java.util.AbstractList;
import java.util.List;

import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * JSON parsing utilities -- mostly on the read-path.
 * <p>
 * For the most part, this deals with the tedium of validating a JSON field's <em>value</em>
 * is the expected type, validating a <em>required</em> JSON field is in fact there, and so on.
 * </p>
 */
public class JsonUtils {

  private JsonUtils() {  }
  
  
  public static String getString(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected '" + name + "' missing");
      return null;
    }
    if (!(value instanceof String))
      throw new JsonParsingException("'" + name + "' expects a simple string: " + value);
    return value.toString();
  }
  
  
  public static String getString(
      JSONObject jObj, String name, String defaultVal) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null)
      return defaultVal;
    
    if (!(value instanceof String))
      throw new JsonParsingException("'" + name + "' expects a simple string: " + value);
    return value.toString();
  }
  
  
  public static Number getNumber(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected numeral '" + name + "' missing");
      return null;
    }
    try {
      return (Number) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a numeral: " + value, ccx);
    }
  }
  
  
  public static double getDouble(JSONObject jObj, String name, double defaultVal) {
    Number value = getNumber(jObj, name, false);
    return value == null ? defaultVal : value.doubleValue();
  }
  
  
  public static float getFloat(JSONObject jObj, String name, float defaultVal) {
    return (float) getDouble(jObj, name, defaultVal);
  }
  
  
  public static long getLong(JSONObject jObj, String name, long defaultVal) {
    Number value = getNumber(jObj, name, false);
    return value == null ? defaultVal : value.longValue();
  }
  
  
  public static int getInt(JSONObject jObj, String name, int defaultVal) {
    return (int) getLong(jObj, name, defaultVal);
  }
  
  
  /**
   * Returns the required named integer.
   */
  public static int getInt(JSONObject jObj, String name) throws JsonParsingException {
    return getNumber(jObj, name, true).intValue();
  }
  
  
  public static boolean getBoolean(JSONObject jObj, String name, boolean defaultVal) {
    Object value = jObj.get(name);
    if (value == null)
      return defaultVal;
    try {
      return ((Boolean) value).booleanValue();
    } catch (ClassCastException ccx) {
      throw new JsonParsingException(
          "expected boolean value for '" + name + "'; actual was " + value);
    }
  }
  
  
  /**
   * Returns the named {@code JSONArray}, if found.
   * 
   * @param require   if {@code true}, then "not-found" is a parsing error
   * @return possibly {@code null}; unless {@code require} is {@code true}
   * 
   * @throws JsonParsingException if {@code require} is {@code true} and no such object is found, or if the named object is not a JSON map
   */
  public static JSONArray getJsonArray(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected JSON array '" + name + "' missing");
      return null;
    }
    try {
      return (JSONArray) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a JSON array: " + value, ccx);
    }
  }
  
  public static List<?> getList(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected JSON array '" + name + "' missing");
      return null;
    }
    try {
      return (List<?>) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a JSON array: " + value, ccx);
    }
  }
  
  /**
   * Returns the named {@code JSONObject}, if found.
   * 
   * @param require   if {@code true}, then "not-found" is a parsing error
   * @return possibly {@code null}; unless {@code require} is {@code true}
   * 
   * @throws JsonParsingException if {@code require} is {@code true} and no such object is found, or if the named object is not a JSON map
   */
  public static JSONObject getJsonObject(JSONObject jObj, String name, boolean require) throws JsonParsingException {
    Object value = jObj.get(name);
    if (value == null) {
      if (require)
        throw new JsonParsingException("expected JSON object '" + name + "' missing");
      return null;
    }
    try {
      return (JSONObject) value;
    } catch (ClassCastException ccx) {
      throw new JsonParsingException("'" + name + "' expects a JSON object: " + value, ccx);
    }
  }
  
  
  
  public static boolean putNotNull(JSONObject jObj, String name, Object value) {
    if (value == null)
      return false;
    jObj.put(name, value);
    return true;
  }
  
  public static boolean putPositive(JSONObject jObj, String name, float value) {
    if (value > 0) {
      jObj.put(name, value);
      return true;
    }
    return false;
  }
  
  public static boolean putNonnegative(JSONObject jObj, String name, float value) {
    if (value >= -0.0f) {
      jObj.put(name, value);
      return true;
    }
    return false;
  }
  
  
  public static boolean putNonzero(JSONObject jObj, String name, float value) {
    if (value > 0 || value < -0.0f) {
      jObj.put(name, value);
      return true;
    }
    return false;
  }
  
  
  public static List<Number> toNumbers(JSONArray jArray) {
    return toNumbers(jArray, true);
  }
  
  
  public static List<Number> toNumbers(JSONArray jArray, boolean strict) {
    return new AbstractList<Number>() {
      @Override
      public Number get(int index) {
        var obj = jArray.get(index);
        if (obj instanceof Number)
          return (Number) obj;
        else if (strict)
          throw new IllegalStateException("element [" + index + "] is not a number: " + obj);
        return null;
      }

      @Override
      public int size() {
        return jArray.size();
      }
    };
  }

}
