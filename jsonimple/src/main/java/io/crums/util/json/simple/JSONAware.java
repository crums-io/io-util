/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json.simple;

/**
 * Beans that support customized output of JSON text shall implement this interface. 
 * 
 * @author Fang Yidong
 */
public interface JSONAware {
  /**
   * @return JSON text
   */
  String toJSONString();
}
