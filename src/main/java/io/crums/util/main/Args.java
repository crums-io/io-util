/*
 * Copyright 2017 Babak Farhang
 */
package io.crums.util.main;

import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 */
public class Args {
  
  private final static char EQ = '=';
  private final static String[] HELP = { "help", "-help", "--help", "-h" };

  private Args() {  }
  
  

  public static String getValue(String[] args, String name) {
    return getValue(args, name, null);
  }
  
  
  public static String getValue(String[] args, String name, String defaultValue) {
    String searchString = name.toString() + EQ;
    for (int i = 0; i < args.length; ++i) {
      if (args[i].startsWith(searchString))
        return args[i].substring(searchString.length());
    }
    return defaultValue;
  }
  
  public static String[] getValues(String[] args, String name) {
    String searchString = name.toString() + EQ;
    ArrayList<String> values = new ArrayList<>(args.length);
    for (String arg : args) {
      if (arg.startsWith(searchString))
        values.add(arg.substring(searchString.length()));
    }
    
    return values.toArray(new String[values.size()]);
  }
  
  
  public static boolean getTrue(String[] args, String name) {
    return "true".equalsIgnoreCase(getValue(args, name, null));
  }
  
  
  public static String orderCharOptions(String optValue, String chars) {
    
    SortedSet<Character> options = new TreeSet<>();
    for (int index = 0; index < optValue.length(); ++index) {
      char c = optValue.charAt(index);
      if (chars.indexOf(c) == -1)
        throw new IllegalArgumentException(
            "illegal option '" + c + "': " + optValue);
      if (!options.add(c))
        throw new IllegalArgumentException(
            "option '" + c + "' specified multiple times: " + optValue);
    }
    StringBuilder s = new StringBuilder(options.size());
    for (char c : options)
      s.append(c);
    return s.toString();
  }



  public static int getIntValue(String[] args, String name, int defaultValue) {
    String value = getValue(args, name, null);
    try {
      return value == null ? defaultValue : Integer.parseInt(value);
    } catch (NumberFormatException nfx) {
      throw new IllegalArgumentException("on parsing " + name + "=" + value, nfx);
    }
  }
  
  
  public static boolean containsAny(String[] args, String... targets) {
    for (String target : targets)
      if (contains(args, target))
        return true;
    return false;
  }
  
  public static boolean contains(String[] args, String target) {
    for (String arg : args)
      if (arg.equals(target))
        return true;
    return false;
  }
  
  
  public static boolean help(String[] args) {
    return containsAny(args, HELP);
  }

}
