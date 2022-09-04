/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json.simple;

import java.io.IOException;
import java.io.Writer;

/**
 * Beans that support customized output of JSON text to a writer shall implement this interface.  
 * @author Fang Yidong
 */
public interface JSONStreamAware {
  /**
   * write JSON string to out.
   */
  void writeJSONString(Writer out) throws IOException;
}
