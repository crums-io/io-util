/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Utility for organizing a properties file with some semblance of order.
 * This subclass allows you to specify the exact order certain property values are written
 * to a file.
 * 
 * <h2>Model</h2>
 * <p>
 * At {@linkplain #TidyProperties(List) construction} an list of unique property names
 * defines the order any name/value pair will appear in the file. The general ordering
 * works as follows:
 * </p>
 * <ul>
 * <li><em>Ordered properties occur in one block.</em> Within this block, properties are
 * ordered as their names occur in {@linkplain #getOrderedNames()}.</li>
 * <li><em>"Unordered" properties occur in one block.</em> Properties whose names do not
 * fall in the {@linkplain #getOrderedNames()} set are ordered lexicographically in one
 * (separate) block.</li>
 * <li>Finally, the above 2 blocks may be ordered 2 ways: ordered properties, first or
 * last.</li>
 * </ul>
 * 
 * <h2>Implementation Note</h2>
 * <p>
 * No effort was made to make this efficient. If the ordered names list is longer than
 * say a 1000, then certain speedups maybe called for since it performs <b>0</b>(N<sup>2</sup>).
 * I hope never to see a config file with that many values, so no bother.
 * </p>
 * @see #TidyProperties(List, boolean, Properties)
 * @see #entrySet()
 */
@SuppressWarnings("serial")
public class TidyProperties extends Properties {
  
  
  
  /**
   * Returns a sub-{@linkplain Properties} containing only those names starting
   * with the given {@code prefix}, and then with the prefix stripped from the
   * name. This is a metaphor for nesting properties (in the same way a JSON
   * or XML element doesn't care about its parent).
   * 
   * @param props non-null
   * @param prefix non-null. Should be "dotted"; if a trailing dot is not present
   *               (and if not empty), then a '.' is appended
   * 
   * @return usually a new sub-properties. However, if either argument is empty
   *         then {@code props} is returned
   */
  public static Properties subProperties(Properties props, String prefix) {
    Objects.requireNonNull(props, "null props");
    Objects.requireNonNull(prefix, "null prefix");
    if (prefix.isEmpty() || props.isEmpty())
      return props;
    Properties sub = new Properties();
    if (!prefix.endsWith("."))
      prefix += ".";
    final int prefixLen = prefix.length();
    for (var e : props.entrySet()) {
      String key = e.getKey().toString();
      if (key.startsWith(prefix)) {
        String subkey = key.substring(prefixLen);
        if (subkey.isEmpty())
          throw new IllegalArgumentException("props contains illegal name: " + key);
        sub.put(subkey, e.getValue());
      }
    }
    return sub;
  }
  
  
  
  private final List<String> orderedNames;
  
  private final boolean first;

  /**
   * Creates an empty instance. Name/value pairs from {@code orderedNames} come first.
   * 
   * @param orderedNames list of parameter names that will be kept in order (no dups)
   */
  public TidyProperties(List<String> orderedNames) {
    this(orderedNames, true);
  }

  /**
   * Creates an empty instance.
   * 
   * @param orderedNames list of parameter names that will be kept in order (no dups)
   * @param first if {@code true}, then name/value pairs from {@code orderedNames} are written first;
   *              otherwise, they're written last
   */
  public TidyProperties(List<String> orderedNames, boolean first) {
    super();
    Objects.requireNonNull(orderedNames, "null orderedNames");
    this.orderedNames = Lists.readOnlyCopy(orderedNames, true);
    this.first = first;
  }

  /**
   * Creates an instance with the given backing defaults. Name/value pairs from {@code orderedNames} come first.
   * Use this as a "promotion constructor".
   * 
   * @param orderedNames list of parameter names that will be kept in order.
   * @param defaults default name/value pairs
   */
  public TidyProperties(List<String> orderedNames, Properties defaults) {
    this(orderedNames, true, defaults);
  }

  /**
   * Creates an instance with the given backing defaults. Use this as a "promotion constructor".
   * 
   * @param orderedNames list of parameter names that will be kept in order.
   * @param first if {@code true}, then name/value pairs from {@code orderedNames} are written first;
   *              otherwise, they're written last
   * @param defaults default name/value pairs
   */
  public TidyProperties(List<String> orderedNames, boolean first, Properties defaults) {
    super(defaults);
    Objects.requireNonNull(orderedNames, "null orderedNames");
    this.orderedNames = Lists.readOnlyCopy(orderedNames, true);
    this.first = first;
  }
  
  

  

  /**
   * <p>This override is the hook that controls the order name/value pairs are written.</p>
   * {@inheritDoc}
   * 
   * @return an ordered enumeration of the existing keys
   */
  @Override
  public Set<Map.Entry<Object, Object>> entrySet() {
    var baseEntries = super.entrySet();
    TreeMap<Object, Object> orderedMap = new TreeMap<>(newComparator());
    for (var e : baseEntries) {
      orderedMap.put(e.getKey(), e.getValue());
    }
    return orderedMap.entrySet();
  }
  

  /**
   * Returns a comparator that orders keys (property names) in the order specified at construction.
   * 
   * @see #getOrderedNames()
   * @see #isFirst()
   */
  protected Comparator<Object> newComparator() {
    return new Comparator<Object>() {
      @Override
      public int compare(Object a, Object b) {
        int aIndex = orderedNames.indexOf(a);
        int bIndex = orderedNames.indexOf(b);
        if (aIndex == -1) {
          if (bIndex == -1)
            return a.toString().compareTo(b.toString());
          else
            return first ? 1 : -1;
        } else if (bIndex == -1) {
          return first ? -1 : 1;
        } else {
          return aIndex - bIndex;
        }
      }
    };
  }

  /**
   * Returns the ordered names.
   */
  public final List<String> getOrderedNames() {
    return orderedNames;
  }

  /**
   * Determines whether the ordered names come first (or last).
   * 
   * @return {@code true} if the name/value pairs from the ordered names come first;
   *         {@code false}, if they come last
   */
  public final boolean isFirst() {
    return first;
  }

}
