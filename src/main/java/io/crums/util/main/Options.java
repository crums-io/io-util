/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.main;


import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Support for groupable options on the command line. E.g. given declared
 * options <em>-a</em> and <em>-b</em>, support parsing these and also
 * <em>-ab</em> and <em>-ba</em>.
 */
public class Options implements Predicate<String> {
  
  
  private final Set<Character> decl;
  
  
  /**
   * Constructor determines which charcters define the option choices.
   * 
   * @param chars (exclude the '-'), no whitespace obviously
   */
  public Options(String chars) {
    if (Objects.requireNonNull(chars, "null chars").isEmpty())
      throw new IllegalArgumentException("empty chars");
    
    this.decl = sort(chars);
    
    if (decl.size() != chars.length())
      throw new IllegalArgumentException("duplicate chars: " + chars);
  }
  
  
  
  private Set<Character> sort(CharSequence chars) {
    if (chars.length() == 1)
      return Collections.singleton(chars.charAt(0));
    
    TreeSet<Character> options = new TreeSet<>();
    for (int index = 0; index < chars.length(); ++index)
      options.add(chars.charAt(index));
    return Collections.unmodifiableSet(options);
  }
  
  
  /**
   * Determines if the given argument if of the form <em>-xzy</em>,
   * <em>-yz</em>, <em>-zx</em>, <em>-x</em>, and so on (where
   * {@code xyz} was the argument at construction).
   */
  @Override
  public boolean test(String arg) {
    if (arg == null || arg.length() < 2 || arg.charAt(0) != '-')
      return false;
    
    var sorted = sort(arg.substring(1));
    return sorted.size() + 1 == arg.length() && decl.containsAll(sorted);
  }
  
  
  /**
   * Composes the given matched arguments into cannonical form.
   * 
   * @param matched
   * 
   * @return  {@code null} if matched is null or empty; otherwise
   *          a string with the characters in alphabetical order (sans the leading '-')
   */
  public String compose(List<String> matched) {
    if (matched == null || matched.isEmpty())
      return null;

    StringBuilder agg = new StringBuilder();
    matched.forEach(m -> agg.append(m.substring(0)));
    
    var sorted = sort(agg);
    if (sorted.size() < agg.length())
      throw new IllegalArgumentException("ambiguous dashed options: " + matched);
    
    return toString(sorted);
  }
  
  
  /**
   * Return the cannonical form of the given {@linkplain #test(String) test}ed argument.
   * 
   * @param opt a string of the form {@code -abc}
   * 
   * @return a string with the characters in alphabetical order (sans the leading '-')
   */
  public String toCannonical(String opt) {
    var sorted = toCannonicalTree(opt);
    return toString(sorted);
  }
  
  
  private String toString(Set<Character> string) {
    StringBuilder s = new StringBuilder(string.size());
    for (char c : string)
      s.append(c);
    return s.toString();
    
  }
  
  
  private Set<Character> toCannonicalTree(String opt) {
    if (Objects.requireNonNull(opt, "null opt").length() < 2 || opt.charAt(0) != '-')
      throw new IllegalArgumentException("opt: " + opt);
    
    var sorted = sort(opt.substring(1));
    if (sorted.size() + 1 != opt.length() || !decl.containsAll(sorted))
      throw new IllegalArgumentException("opt: " + opt);
    return sorted;
  }
  
  
  /**
   * Returns the declared chars.
   * 
   * @return string of strictly ascending chars
   */
  public String getDeclaredChars() {
    return toString(decl);
  }
  
  
  @Override
  public String toString() {
    return "[" + getDeclaredChars() + "]";
  }

}
