/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.main;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import io.crums.util.Lists;

/**
 * A consumable argument list.
 * <p>
 * Elements in returned list are ordered as they occur on the command line array.
 * </p>
 */
public class ArgList {
  
  private final static char EQ = '=';
  
  

  private final String[] args;
  private final ArrayList<String> argsRemaining;

  
  /**
   * Constructor makes its own copy of argument.
   * 
   * @param args command line arguments
   */
  public ArgList(String[] args) {
    this(Lists.asReadOnlyList(args));
  }
  
  
  
  
  /**
   * Constructor makes its own copy of argument.
   * 
   * @param args command line arguments
   */
  public ArgList(List<String> args) {
    this.args = new String[args.size()];
    this.argsRemaining = new ArrayList<>(this.args.length);
    
    for (int index = 0; index < this.args.length; ++index) {
      String arg = args.get(index);
      this.args[index] = arg;
      this.argsRemaining.add(arg);
    }
  }
  
  
  /**
   * Copy constructor. The state of the new instance is initialized
   * to the state of the <tt>copy</tt> but is henceforth independent.
   * 
   * @param copy
   */
  public ArgList(ArgList copy) {
    this.args = copy.args;
    this.argsRemaining = new ArrayList<>(copy.argsRemaining);
  }
  

  
  /**
   * Returns the number of remaining (unconsumed) arguments.
   */
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
  
  
  /**
   * Returns a <em>live</em>, read-only view of the remaining arguments.
   */
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
  
  
  public void enforceNoRemaining() {
    if (!isEmpty())
      throw new IllegalArgumentException(
          "illegal arguments / combination: " + getArgString());
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
  
  
  public String removeRequiredValue(String name) {
    String value = removeValue(name);
    if (value == null)
      throw new IllegalArgumentException("missing required value '" + name + "'");
    return value;
  }
  
  
  /**
   * Removes and returns the first remaining argument, if any; {@code null}, otherwise.
   */
  public String removeFirst() {
    return argsRemaining.isEmpty() ? null : argsRemaining.remove(0);
  }
  
  
  public boolean removeBoolean(String name) {
    return removeBoolean(name, false);
  }
  
  
  
  
  
  public boolean removeBoolean(String name, boolean defaultValue) {
    String value = removeValue(name);
    if (value == null)
      return defaultValue;
    if ("false".equals(value))
      return false;
    else if ("true".equals(value))
      return true;
    throw new IllegalArgumentException(name + EQ + value + " neither 'true' nor 'false'");
  }
  
  
  
  
  public long removeLong(String name, long defaultValue) {
    String value = removeValue(name);
    if (value == null)
      return defaultValue;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException nfx) {
      throw new IllegalArgumentException(name + EQ + value + " not a whole number");
    }
  }
  
  public List<Long> removeNumbers() {
    return removeNumbers(false);
  }
  
  public List<Long> removeNumbers(boolean includeRanges) {
    if (includeRanges) {
      
      List<String> ranges = removeMatched(NumbersArg.MATCHER);
      if (ranges.isEmpty())
        return Collections.emptyList();
      
      ArrayList<Long> out = new ArrayList<>();
      for (String range : ranges)
        NumbersArg.parse(range, out);
      
      return Collections.unmodifiableList(out);
    
    } else {
      List<String> values = removeMatched(this::isNumber);
      return Lists.map(values, Long::parseLong);
    }
  }
  
  
  
  public List<File> removeExistingPaths() {
    return removeExistingPaths(s -> new File(s).exists());
  }
  
  
  
  public List<File> removeExistingFiles() {
    return removeExistingPaths(s -> new File(s).isFile());
  }
  

  /**
   * Removes and returns the single, existing, remaining file specified on the
   * command line.
   * 
   * @return an existing file, if one is found; {@code null} o.w.
   * 
   * @throws IllegalArgumentException if multiple arguments match existing files
   */
  public File removeExistingFile() {
    List<File> file = removeExistingFiles();
    switch (file.size()) {
    case 0: return null;
    case 1: return file.get(0);
    default:
      throw new IllegalArgumentException("ambiguous. Multiple existing files: " + file);
    }
    
  }
  
  
  
  public List<File> removeExistingDirectories() {
    return removeExistingPaths(s -> new File(s).isDirectory());
  }
  
  
  /**
   * Removes and returns the single, existing, remaining directory specified on the
   * command line.
   * 
   * @return an existing directory, if one is found; {@code null} o.w.
   * 
   * @throws IllegalArgumentException if multiple arguments match existing directories
   */
  public File removeExistingDirectory() throws IllegalArgumentException {
    List<File> dir = removeExistingDirectories();
    switch (dir.size()) {
    case 0: return null;
    case 1: return dir.get(0);
    default:
      throw new IllegalArgumentException("ambiguous. Multiple existing directories: " + dir);
    }
  }
  
  
  
  
  
  private List<File> removeExistingPaths(Predicate<String> condition) {
    List<String> paths = removeMatched(condition);
    return Lists.map(paths, p -> new File(p));
  }
  
  
  private boolean isNumber(String value) {
    try {
      Long.parseLong(value);
      return true;
    } catch (NumberFormatException nfx) {
      return false;
    }
  }
  
  
  /**
   * Removes and returns the options gathered that match the given
   * option characters.
   * 
   * @param chars the options (w/o the '-')
   * 
   * @return the gathered options sorted in alphabetical order, if any; {@code null} o.w.
   */
  public String removedDashedOptions(String chars) {
    Options opts = new Options(chars);
    List<String> matched = removeMatched(opts);
    return opts.compose(matched);
  }
  
  
  /**
   * Removes and returns the value in the next <tt>name=<em>value</em></tt> pair,
   * or <tt>null</tt> if the pattern does not occur. There may be multiple of these
   * for the same name. This method consumes the first one.
   * 
   * @param name the name in the <tt>name=<em>value</em></tt> pair
   */
  public String removeValue(String name) {
    String arg = popOrGetNextMatch(s -> s.startsWith(name + EQ), true);
    return substring(arg, name.length() + 1);
  }
  
  
  private String substring(String string, int index) {
    return string == null ? null : string.substring(index);
  }
  
  
  
  public String removeValue(String name, String defaultValue) {
    String value = removeValue(name);
    return value == null ? defaultValue : value;
  }
  
  
  /**
   * Removes and returns a single required command from the given choice of many.
   * 
   * @param command var arg list of commands
   * 
   * @return the command
   * 
   * @throws IllegalArgumentException if no commands or multiple commands are parsed 
   */
  public String removeCommand(String... command) throws IllegalArgumentException {
    List<String> cmd = removeContained(command);
    if (cmd.isEmpty())
      throw new IllegalArgumentException("no command specified");
    if (cmd.size() != 1)
      throw new IllegalArgumentException("only one command at a time may be given: " + cmd);
    return cmd.get(0);
  }
  
  
  public List<String> removeContained(String... args) {
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
  
  
  public List<String> removeContained(Collection<String> args) {
    return removeContained(new HashSet<String>(args));
  }
  
  
  public List<String> removeContained(Set<String> searchStrings) {
    return popOrGetMatches(searchStrings::contains, true);
  }
  
  /**
   * Removes and returns the single argument matching the {@code condition}, if found;
   * {@code null} otherwise.
   * 
   * @throws IllegalArgumentException if multiple arguments match
   */
  public String removeSingle(Predicate<String> condition) throws IllegalArgumentException {
    List<String> matched = removeMatched(condition);
    switch (matched.size()) {
    case 0: return null;
    case 1: return matched.get(0);
    default:
      throw new IllegalArgumentException("ambiguous arguments: " + matched);
    }
  }
  
  
  /**
   * Removes the arguments matching the <tt>condition</tt>, collects and returns them.
   * If on evaluating a condition it throws an exception, the evaluation counts
   * as <tt>false</tt>.
   */
  public List<String> removeMatched(Predicate<String> condition) {
    return popOrGetMatches(s -> condition.test(s), true);
  }
  
  
  // 'pop' here means remove, not really a stack
  private String popOrGetNextMatch(Predicate<String> condition, boolean pop) {
    List<String> m = popOrGetMatches(condition, pop, 1);
    return m.isEmpty() ? null : m.get(0);
  }
  
  private List<String> popOrGetMatches(Predicate<String> condition, boolean pop) {
    return popOrGetMatches(condition, pop, Integer.MAX_VALUE);
  }
  
  private List<String> popOrGetMatches(Predicate<String> condition, boolean pop, int max) {
    ArrayList<String> matches = new ArrayList<>(Math.min(max, size()));
    
    for (int index = 0; index < argsRemaining.size(); ) {
      String arg = argsRemaining.get(index);
      
      boolean hit;
      try {
        hit = condition.test(arg);
      } catch (Exception x) {
        hit = false;
      }
      
      if (hit) {  // a hit
        
        matches.add(arg);
        if (pop)
          argsRemaining.remove(index);
        else
          ++index;
        if (--max == 0)
          break;
      
      } else {    // ! a hit
        ++index;
      }
    }
    return matches;
  }

}















