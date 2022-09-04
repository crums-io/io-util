/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json.simple;

import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.crums.util.json.simple.parser.JSONParser;

/**
 * A JSON object. JSONObject supports java.util.Map interface.
 * 
 * <h2>Changes</h2>
 * <p>
 * Key/value pairs are ordered in insertion order.
 * This uses a swappable {@linkplain Map} implementation. The default is {@linkplain LinkedHashMap},
 * which is <em>slower</em> than a regular {@linkplain HashMap}. However, on the read-path
 * (that is {@linkplain JSONParser}) the library uses the faster {@code HashMap} implementation. 
 * </p>
 * 
 * @author Fang Yidong
 * 
 * @see #newFastInstance() {@code JSONObject.newFastInstance()} &nbsp; for old-style implementation
 */
public class JSONObject extends DelegateMap<Object, Object> implements JSONAware, JSONStreamAware {
  
  
  /**
   * Returns a regular, old-style instance. The order of the key/value pairs is
   * [pseudo-] random.
   * 
   * @return an instance using a {@linkplain HashMap} underneath
   */
  public static JSONObject newFastInstance() {
    return new JSONObject(new HashMap<>(), null);
  }
  
  
  /**
   * Creates an insertion-ordered instance.
   * 
   * @see #newFastInstance()
   */
  public JSONObject() {
    this(new LinkedHashMap<>(), null);
  }

  /**
   * Allows creation of a JSONObject from a Map. After that, both the
   * generated JSONObject and the Map can be modified independently.
   * <p>
   * The instance is insertion-ordered.
   * </p>
   */
  public JSONObject(Map<Object,Object> map) {
    this();
    delegate.putAll(map);
  }
  
  
  /**
   * Base constructor.
   * 
   * @param impl  non-null implementation map
   * @param init  optional initial mappings (may be {@code null})
   */
  protected JSONObject(Map<Object, Object> impl, Map<Object,Object> init) {
    super(impl);
    if (init != null)
      delegate.putAll(impl);
  }


    /**
     * Encode a map into JSON text and write it to out.
     * If this map is also a JSONAware or JSONStreamAware, JSONAware or JSONStreamAware specific behaviours will be ignored at this top level.
     * 
     * @see JSONValue#writeJSONString(Object, Writer)
     * 
     * @param map
     * @param out
     */
  public static void writeJSONString(Map<?, ?> map, Writer out) throws IOException {
    if(map == null){
      out.write("null");
      return;
    }
    
    boolean first = true;
    var iter =map.entrySet().iterator();
    
        out.write('{');
    while(iter.hasNext()){
            if(first)
                first = false;
            else
                out.write(',');
      var entry = iter.next();
            out.write('\"');
            out.write(escape(String.valueOf(entry.getKey())));
            out.write('\"');
            out.write(':');
      JSONValue.writeJSONString(entry.getValue(), out);
    }
    out.write('}');
  }

  public void writeJSONString(Writer out) throws IOException{
    writeJSONString(this, out);
  }
  
  
  public void writeJSONString(Appendable out) throws IOException {
    writeJSONString(new AppendableWriterAdaptor(out));
  }
  
  /**
   * Convert a map to JSON text. The result is a JSON object. 
   * If this map is also a JSONAware, JSONAware specific behaviours will be omitted at this top level.
   * 
   * @see JSONValue#toJSONString(Object)
   * 
   * @param map
   * @return JSON text, or "null" if map is null.
   */
  public static String toJSONString(Map<?, ?> map){
    if(map == null)
      return "null";
    
        var sb = new StringBuilder();
        boolean first = true;
    var iter= map.entrySet().iterator();
    
        sb.append('{');
    while(iter.hasNext()){
            if(first)
                first = false;
            else
                sb.append(',');
            
      var entry = iter.next();
      toJSONString(String.valueOf(entry.getKey()),entry.getValue(), sb);
    }
        sb.append('}');
    return sb.toString();
  }
  
  public String toJSONString(){
    return toJSONString(this);
  }
  
  private static String toJSONString(String key,Object value, StringBuilder sb){
    sb.append('\"');
        if(key == null)
            sb.append("null");
        else
            JSONValue.escape(key, sb);
    sb.append('\"').append(':');
    
    sb.append(JSONValue.toJSONString(value));
    
    return sb.toString();
  }
  
  public String toString(){
    return toJSONString();
  }

  public static String toString(String key,Object value){
        var sb = new StringBuilder();
    toJSONString(key, value, sb);
        return sb.toString();
  }
  
  /**
   * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters (U+0000 through U+001F).
   * It's the same as JSONValue.escape() only for compatibility here.
   * 
   * @see JSONValue#escape(String)
   * 
   */
  public static String escape(String s){
    return JSONValue.escape(s);
  }
  
  
  
  
  
  
  
  
  // support class for writeJSONString(Appendable)
  // copied here from io.crums.util.Maps
  private static class AppendableWriterAdaptor extends Writer {
    
    private final Appendable out;
    private final boolean close;
    
    AppendableWriterAdaptor(Appendable out) {
      Objects.requireNonNull(out);
      this.out = out;
      this.close = out instanceof AutoCloseable;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      out.append(CharBuffer.wrap(cbuf, off, len));
      
    }

    @Override
    public void flush() throws IOException {
      if (out instanceof FilterOutputStream)
        ((FilterOutputStream) out).flush();
      else if (out instanceof Flushable) {
        ((Flushable) out).flush();
      }
    }

    @Override
    public void close() throws IOException {
      if (close) {
        try {
          ((AutoCloseable) out).close();
        } catch (IOException iox) {
          // rethrow
          throw iox;
        } catch (Exception x) {
          throw new IOException("on close: " + x.getMessage(), x);
        }
      }
    }

    @Override
    public void write(int c) throws IOException {
      out.append((char) c);
    }

    @Override
    public void write(String str) throws IOException {
      out.append(str);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
      out.append(str, off, off + len);
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
      out.append(csq);
      return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) throws IOException {
      out.append(csq, start, end);
      return this;
    }

    @Override
    public Writer append(char c) throws IOException {
      out.append(c);
      return this;
    }
    
  }
  
  
  
  
  
  
  
}