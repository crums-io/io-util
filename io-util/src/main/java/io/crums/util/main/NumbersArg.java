/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.main;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.function.Predicate;

import io.crums.util.Lists;

/**
 * Parses a comma-separated list of whole numbers (interpreted as longs) and
 * hyphenated number ranges (inclusive). Parsing errors are signaled as {@code null}
 * return values.
 * 
 * <h2>Example</h2>
 * <p>
 * {@code 0,-2,5-9,6} parses to a list containing the numbers {@code 0 -2 5 6 7 8 9 6}
 * </p><p>
 * Note this works both for positive and negative numbers.. altho, it should be
 * said, ranges involving negative numbers look a bit funky (e.g. {@code -5-9}
 * means -5 thru 9).
 * </p>
 * 
 * <h4>Performance note</h4>
 * 
 * <p>
 * This is inefficient. We're processing command-line args, where this is usually not
 * an issue. I don't have a use case yet, but if really big ranges are desired without
 * consuming much memory, then creating such a view would be relatively straight-forward.
 * </p>
 */
public class NumbersArg {
  
  public final static Predicate<String> MATCHER = s -> parse(s, Lists.sink()) != null;

  // nobody calls
  private NumbersArg() {  }
  
  
  /**
   * Token separator. Each token specifies either a single number, or a range of
   * numbers.
   */
  public final static char COMMA = ',';
  /**
   * Range lo-hi separator.
   */
  public final static char DASH = '-';

  /**
   * Default maximum size of a range. Used as a sanity check so we don't run out
   * of memory on bad input. (It's not supposed to be mem-intensive anyway.)
   */
  public final static int DEFAULT_MAX_RANGE = 256 * 256;
  /**
   * Minimum valid setting for the {@code maxRange} parameter.
   * 
   * @see #parse(String, int)
   */
  public final static int MIN_RANGE = 3;
  
  private final static String COMMA_TOKEN_DEF = new String(new char[] { COMMA });

  
  

  /**
   * Parses a list of longs from the given argument string and returns it if successful;
   * returns null otherwise.
   * 
   * @param arg argument string
   * 
   * @return null or mutable list returned by {@code parse(arg, DEFAULT_MAX_RANGE)}
   * 
   * @see #DEFAULT_MAX_RANGE
   */
  public static List<Long> parse(String arg) {
    return parse(arg, DEFAULT_MAX_RANGE, null);
  }
  
  

  public static List<Long> parse(String arg, List<Long> out) {
    return parse(arg, DEFAULT_MAX_RANGE, out);
  }
  
  

  /**
   * Parses a list of ints from the given argument string and returns it if successful;
   * returns null otherwise.
   * 
   * @param arg argument string
   * 
   * @return null or immutable list
   * 
   * @see #DEFAULT_MAX_RANGE
   */
  public static List<Integer> parseInts(String arg) {
    List<Long> longs = parse(arg);
    return longs == null ? null : Lists.map(longs, lng -> lng.intValue());
  }
  
  
  
  /**
   * Parses a list of longs from the given argument string and returns it if successful;
   * returns null otherwise.
   * 
   * @param arg argument string
   * @param maxRange enforces the maximum size of a range specification (so we don't run
   *          out of memory on bad input). Must be &ge; {@linkplain #MIN_RANGE}
   * 
   * @return null or mutable list
   */
  public static List<Long> parse(String arg, final int maxRange, List<Long> out) {
    
    if (maxRange < MIN_RANGE)
      throw new IllegalArgumentException("maxRange " + maxRange + " < minimum " + MIN_RANGE);
    
    arg = Objects.requireNonNull(arg, "null arg").trim();
    if (arg.isEmpty())
      return null;
    
    // comma-separated tokens
    ArrayList<String> cst = new ArrayList<>();
    for (
        StringTokenizer tokenizer = new StringTokenizer(arg, COMMA_TOKEN_DEF);
        tokenizer.hasMoreTokens();
        cst.add(tokenizer.nextToken().trim()));
    
    List<Long> nums = out == null ? new ArrayList<>(Math.max(8, cst.size())) : out;
    
    try {
      for (String token : cst) {
        // see if there's a range to parse
        int cindex = token.indexOf(DASH);
        
        // if it's a single number extract it and continue,
        // but be careful, we might have negative numbers..
        // (I don't have a use case for this yet, but easy enuf to implement)
        if (cindex < 1) {
          // (if zero, then interpret as a negative number)
          if (cindex == 0) {
            // see if another dash follows (range definition starting from a negative value)
            cindex = token.indexOf(DASH, 1);
            if (cindex == -1) {
              // parse negative number
              nums.add(Long.parseLong(token));
              continue;
            }     // else found a 2nd dash.. fall thru to range parsing
          
          } else {
            // cindex == -1 ..
            // parse positive number
            nums.add(Long.parseLong(token));
            continue;
          }
        }
        long first = Long.parseLong(token.substring(0, cindex));
        long last = Long.parseLong(token.substring(cindex + 1));
        
        // check range order and size
        if (first > last || last - first > maxRange)
          return null;
        
        for (long next = first; next <= last; nums.add(next++));
      }
      
      
      return nums;
    } catch (Exception parsingException) {
      // eat it
      return null;
    }
  }
  
  
  /**
   * Parses a list of {@code long}s from the given argument string and returns
   * them as a list of unique, ascending {@code long}s if successful;
   * returns null otherwise.
   * 
   * @param arg
   * @return null or immutable list.
   */
  public static List<Long> parseSorted(String arg) {
    var unsorted = parse(arg);
    return unsorted == null ? null : Lists.sortRemoveDups(unsorted);
  }

}
