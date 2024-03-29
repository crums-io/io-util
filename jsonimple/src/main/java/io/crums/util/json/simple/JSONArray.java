/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json.simple;


import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;


/**
 * A JSON array. JSONObject supports java.util.List interface.
 * 
 * @author Fang Yidong
 */
public class JSONArray extends ArrayList<Object> implements JSONAware, JSONStreamAware {
  private static final long serialVersionUID = 3957988303675231981L;
  
  

    /**
     * Encode a list into JSON text and write it to out. 
     * If this list is also a JSONStreamAware or a JSONAware, JSONStreamAware and JSONAware specific behaviours will be ignored at this top level.
     * 
     * @see JSONValue#writeJSONString(Object, Writer)
     * 
     * @param list
     * @param out
     */
  public static void writeJSONString(List<?> list, Writer out) throws IOException{
    if(list == null){
      out.write("null");
      return;
    }
    
    boolean first = true;
    var iter = list.iterator();
    
        out.write('[');
    while(iter.hasNext()){
            if(first)
                first = false;
            else
                out.write(',');
            
      Object value=iter.next();
      if(value == null){
        out.write("null");
        continue;
      }
      
      JSONValue.writeJSONString(value, out);
    }
    out.write(']');
  }
  
  public void writeJSONString(Writer out) throws IOException{
    writeJSONString(this, out);
  }
  
  /**
   * Convert a list to JSON text. The result is a JSON array. 
   * If this list is also a JSONAware, JSONAware specific behaviours will be omitted at this top level.
   * 
   * @see JSONValue#toJSONString(Object)
   * 
   * @param list
   * @return JSON text, or "null" if list is null.
   */
  public static String toJSONString(List<?> list){
    if(list == null)
      return "null";
    
        boolean first = true;
        var sb = new StringBuilder();
    var iter = list.iterator();
        
        sb.append('[');
    while(iter.hasNext()){
            if(first)
                first = false;
            else
                sb.append(',');
            
      Object value=iter.next();
      if(value == null){
        sb.append("null");
        continue;
      }
      sb.append(JSONValue.toJSONString(value));
    }
        sb.append(']');
    return sb.toString();
  }
  
  
  public JSONArray() {
  }
  
  
  public JSONArray(int initCapacity) {
    super(initCapacity);
  }
  
  
  

  public String toJSONString(){
    return toJSONString(this);
  }
  
  public String toString() {
    return toJSONString();
  }

  
    
}
