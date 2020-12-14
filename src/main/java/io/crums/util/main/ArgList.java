/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.main;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import io.crums.util.Lists;

/**
 * A consumable argument list.
 */
public class ArgList {
  
  private final static char EQ = '=';
  
  

  private final String[] args;
  private final ArrayList<String> argsRemaining;

  /**
   * 
   */
  public ArgList(String[] args) {
    this.args = new String[args.length];
    this.argsRemaining = new ArrayList<>(args.length);
    
    for (int index = 0; index < args.length; ++index) {
      String arg = args[index];
      this.args[index] = arg;
      this.argsRemaining.add(arg);
    }
  }
  

  
  
  public int size() {
    return argsRemaining.size();
  }
  
  public final boolean isEmpty() {
    return size() == 0;
  }
  
  
  public List<String> args() {
    return Lists.asReadOnlyList(args);
  }
  
  
  public String getArgString() {
    if (args.length == 0)
      return "";
    StringBuilder s = new StringBuilder();
    for (String arg : args)
      s.append(arg).append(' ');
    return s.substring(0, s.length() - 1);
  }
  
  
  public List<String> argsRemaining() {
    return Collections.unmodifiableList(argsRemaining);
  }
  
  
  public String getArgsRemainingString() {
    if (isEmpty())
      return "";

    StringBuilder s = new StringBuilder();
    for (String arg : argsRemaining)
      s.append(arg).append(' ');
    return s.substring(0, s.length() - 1);
  }
  

  /**
   * 
   * @param name
   * @return
   */
  public String getValue(String name) {
    String arg = popOrGetNextMatch(s -> s.startsWith(name + EQ), false);
    return substring(arg, name.length() + 1);
  }
  
  
  public String getValue(String name, String defaultValue) {
    String value = getValue(name);
    return value == null ? defaultValue : value;
  }
  
  
  public String popRequiredValue(String name) {
    String value = popValue(name);
    if (value == null)
      throw new IllegalArgumentException("missing required value '" + name);
    return value;
  }
  
  
  public boolean popBoolean(String name) {
    return popBoolean(name, false);
  }
  
  
  public boolean popBoolean(String name, boolean defaultValue) {
    String value = popValue(name);
    if (value == null)
      return defaultValue;
    if ("false".equals(value))
      return false;
    else if ("true".equals(value))
      return true;
    throw new IllegalArgumentException(name + EQ + value + " neither 'true' nor 'false'");
  }
  
  
  
  
  public long popLong(String name, long defaultValue) {
    String value = popValue(name);
    if (value == null)
      return defaultValue;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException nfx) {
      throw new IllegalArgumentException(name + EQ + value + " not a whole number");
    }
  }
  
  
  public List<Long> popNumbers() {
    List<String> values = removeMatched(this::isNumber);
    return Lists.map(values, Long::parseLong);
  }
  
  
  private boolean isNumber(String value) {
    try {
      Long.parseLong(value);
      return true;
    } catch (NumberFormatException nfx) {
      return false;
    }
  }
  
  
  public String popValue(String name) {
    String arg = popOrGetNextMatch(s -> s.startsWith(name + EQ), true);
    return substring(arg, name.length() + 1);
  }
  
  
  private String substring(String string, int index) {
    return string == null ? null : string.substring(index);
  }
  
  
  
  public String popValue(String name, String defaultValue) {
    String value = popValue(name);
    return value == null ? defaultValue : value;
  }
  
  
  
  public List<String> removedContained(String... args) {
    Set<String> argset;
    int len = args.length;
    switch (len) {
    case 0:   return Collections.emptyList();
    case 1:   argset = Collections.singleton(args[0]); break;
    default:
      argset = new HashSet<>(len);
      argset.addAll(Arrays.asList(args));
    }
    return removeContained(argset);
  }
  
  public List<String> removeContained(Set<String> searchStrings) {
    return popOrGetMatches(searchStrings::contains, true);
  }
  
  
  public List<String> removeMatched(Function<String, Boolean> condition) {
    return popOrGetMatches(condition, true);
  }
  
  
  
  
  
  
  private String popOrGetNextMatch(Function<String, Boolean> condition, boolean pop) {
    List<String> m = popOrGetMatches(condition, pop, 1);
    return m.isEmpty() ? null : m.get(0);
  }
  
  private List<String> popOrGetMatches(Function<String, Boolean> condition, boolean pop) {
    return popOrGetMatches(condition, pop, Integer.MAX_VALUE);
  }
  
  private List<String> popOrGetMatches(Function<String, Boolean> condition, boolean pop, int max) {
    ArrayList<String> matches = new ArrayList<>(Math.min(max, size()));
    
    for (int index = 0; index < argsRemaining.size(); ) {
      String arg = argsRemaining.get(index);
      
      if (condition.apply(arg)) {  // a hit
        
        matches.add(arg);
        if (pop)
          argsRemaining.remove(index);
        else
          ++index;
        if (--max == 0)
          break;
      
      } else {                    // ! a hit
        ++index;
      }
    }
    return matches;
  }

}















