/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A pretty-printer for JSON. This always prints with indentation (by default, 2 spaces).
 * 
 * 
 * <h2>Historical Note</h2>
 * <p>
 * Since the JSON library I was using ({@code json-simple-1.1.1})
 * did not support pretty printing, on looking at their code I realized
 * I could adapt it without having to use any of its constructs directly.
 * (As it turned out, I brought in the entirety of the small library.)
 * </p><p>
 * Because of the original library's clean design ({@code JSONObject} is
 * a {@code Map}, and {@code JSONArray} is a {@code List}) it's objects
 * work out-of-the-box with this class.
 * </p>
 * <h2>Single Thread Use</h2>
 * <p>
 * Instances are <em>not suitable for use under concurrent access!</em>
 * The static methods are obviously OK under concurrent access, tho you
 * still should take care not to interleave writing to the same streams.
 * </p>
 */
public class JsonPrinter {
  
  
  /** Returns the JSON map as indented string. */
  public static String toJson(Map<?,?> map) {
    StringBuilder out = new StringBuilder(32);
    new JsonPrinter(out).print(map);
    return out.toString();
  }
  

  /** Returns the JSON list as indented string. */
  public static String toJson(List<?> list) {
    StringBuilder out = new StringBuilder(32);
    new JsonPrinter(out).print(list);
    return out.toString();
  }
  

  /** Shortcut for {@code println(map, System.out)}. */
  public static void println(Map<?,?> map) {
    println(map, System.out);
  }
  
  /** Shortcut for {@code println(list, System.out)}. */
  public static void println(List<?> list) {
    println(list, System.out);
  }
  
  
  /** Prints {@code map} to the stream {@code out}. */
  public static void println(Map<?,?> map, PrintStream out) {
    new JsonPrinter(out).print(map);
    out.println();
  }
  

  /** Prints {@code list} to the stream {@code out}. */
  public static void println(List<?> list, PrintStream out) {
    new JsonPrinter(out).print(list);
    out.println();
  }
  
  
  
  /**
   * Writes the given JSON {@code map} to the specified filepath.
   * 
   * @param file      must not exist; it's parent directory must exist
   */
  public static void write(Map<?,?> map, File file) throws UncheckedIOException {
    writeImpl(JsonPrinter::println, map, file);
  }

  /**
   * Writes the given JSON {@code list} to the specified filepath.
   * 
   * @param file      must not exist; it's parent directory must exist
   */
  public static void write(List<?> list, File file) throws UncheckedIOException {
    writeImpl(JsonPrinter::println, list, file);
  }
  
  
  
  private static <T> void writeImpl(BiConsumer<T, PrintStream> writer, T jObj, File file) throws UncheckedIOException {
    if (file.exists())
      throw new IllegalArgumentException("cannot overwrite existing file: " + file);
    
    try (var out = new PrintStream(file, UTF_8)) {
      
      writer.accept(jObj, out);
      
    } catch (IOException iox) {
      throw new UncheckedIOException("on writing to " + file, iox);
    }
  }
  private final static Charset UTF_8 = Charset.forName("UTF-8");
  
  
  
  
  private final Appendable out;
  private final String indentUnit;
  private final String newLine;
  
  
  private int indents;
  
  /**
   * Constructs an instance with 2 spaces for indentation and the system line separator
   * (windows or unix flavor).
   * 
   * @param out         typically a {@linkplain PrintStream} or a {@linkplain StringBuilder}
   */
  public JsonPrinter(Appendable out) {
    this(out, "  ", System.lineSeparator());
  }


  /**
   * Full constructor.
   * 
   * @param out         typically a {@linkplain PrintStream} or a {@linkplain StringBuilder}
   * @param indentUnit  must be blank. Default is 2 spaces.
   * @param newLine     either {@code '\r\n'} or {@code '\n'}
   */
  public JsonPrinter(Appendable out, String indentUnit, String newLine) {
    this.out = Objects.requireNonNull(out, "null out");
    this.indentUnit = Objects.requireNonNull(indentUnit, "null indentUnit");
    this.newLine = Objects.requireNonNull(newLine, "null newLine");
    
    if (!indentUnit.isBlank())
      throw new IllegalArgumentException(
          "indentUnit must be a blank sequence (quoted): '" + indentUnit + "'");
    if (!"\r\n".equals(newLine) && ! "\n".equals(newLine))
      throw new IllegalArgumentException(
          "unrecognized newLine (quoted): '" + newLine + "'");
  }
  
  
  public void print(List<?> list) throws UncheckedIOException {
    try {
      assert indents == 0;
      printImpl(list);
      assert indents == 0;
    } catch (IOException iox) {
      throw new UncheckedIOException("on print(List): " + list, iox);
    }
  }
  
  
  public void print(Map<?,?> map) throws UncheckedIOException {
    try {
      assert indents == 0;
      printImpl(map);
      assert indents == 0;
    } catch (IOException iox) {
      throw new UncheckedIOException("on print(Map): " + map, iox);
    }
  }
  
  
  protected void printImpl(List<?> list) throws IOException {
    if (list == null) {
      out.append("null");
      return;
    }
    
    
    boolean first = true;
    var iter = list.iterator();
    
    open('[');
    while (iter.hasNext()) {
      if (first)
        first = false;
      else
        out.append(',');
      
      out.append(newLine);
      appendIndents();
      
      appendValue(iter.next());
    }
    close(']');
  }
  
  
  protected void printImpl(Map<?, ?> map) throws IOException {
    if (map == null) {
      out.append("null");
      return;
    }
    
    
    boolean first = true;
    var iter = map.entrySet().iterator();
    
    open('{');
    while (iter.hasNext()) {
      if (first)
        first = false;
      else
        out.append(',');
      
      out.append(newLine);
      appendIndents();
      
      var entry = iter.next();
      print(entry.getKey().toString(), entry.getValue());
    }
    close('}');
  }
  
  
  private void open(char c) throws IOException {
    out.append(c);
    ++indents;
  }
  
  private void close(char c) throws IOException {
    --indents;
    out.append(newLine);
    appendIndents();
    out.append(c);
  }
  
  private void appendIndents() throws IOException {
    for (int count = indents; count-- > 0; )
      out.append(indentUnit);
  }
  
  
  
  private void print(String key, Object value) throws IOException {
    out.append('"');
    appendString(key);
    out.append('"').append(':').append(' ');
    appendValue(value);
  }
  
  
  
  private void appendString(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      switch(ch){
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      case '/':
        out.append('/');
        break;
      default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
        if ((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')) {
          String ss = Integer.toHexString(ch);
          out.append("\\u");
          for (int k=0; k < 4-ss.length(); k++) {
            out.append('0');
          }
          out.append(ss.toUpperCase());
        }
        else{
          out.append(ch);
        }
      }
    }//for
  }
  
  
  private void appendValue(Object value) throws IOException {
    if (value == null) {
      out.append("null");
      
    } else if (value instanceof String) {
      out.append('"');
      appendString(value.toString());
      out.append('"');
      
    } else if (value instanceof Double) {
      var num = (Double) value;
      if (num.isInfinite() || num.isNaN())
        out.append("null");
      else
        out.append(value.toString());
      
    } else if (value instanceof Float) {
      var num = (Float) value;
      if (num.isInfinite() || num.isNaN())
        out.append("null");
      else
        out.append(value.toString());
      
    } else if (value instanceof Map) {
      printImpl((Map<?,?>) value);
      
    } else if (value instanceof List) {
      printImpl((List<?>) value);
      
//    } else if (value instanceof Number || value instanceof Boolean) {
//      out.append(value.toString());
      
    } else {
      out.append(value.toString());
      
    }
  }

}
