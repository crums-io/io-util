/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.main;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import io.crums.util.DelegateEquivalent;
import io.crums.util.Strings;

/**
 * Convention for <em>switch</em> options on the command line. An option of the form
 * {@code --Myopt} is also understood in short form {@code -M}.
 * 
 * <h2>Design Note</h2>
 * <p>
 * In the convention I'm adopting, argument order does not matter. Command line
 * options that take arguments are not dashed; instead they're spec'ed out as a
 * {@code name=}<em>{@code value}</em> pair.
 * </p>
 */
public class Option extends DelegateEquivalent implements Predicate<String> {
  
  private final String name;
  
  
  /**
   * @param name sans dashes, ASCII alphabet-only of length &ge; 2
   */
  public Option(String name) {
    this.name = Objects.requireNonNull(name, "null name");
    if (name.length() < 2)
      throw new IllegalArgumentException("name (quoted): '" + name + "'");
    for (int index = name.length(); index-- > 0; ) {
      char c = name.charAt(index);
      if (Strings.isAlphabet(c))
        continue;
      if (index == 0 || (c != '-' && c != '_'))
        throw new IllegalArgumentException("name (quoted): '" + name + "'");
    }
  }
  
  
  /**
   * Returns the name (sans dashes).
   * 
   * @see #getFull()
   */
  public final String getName() {
    return name;
  }
  
  /**
   * Returns the full option.
   * 
   * @return {@code "--" + getName()}
   */
  public final String getFull() {
    return "--" + name;
  }
  
  
  /**
   * Returns the short form symbol.
   * 
   * @return {@code getName().charAt(0)}
   */
  public final char getSym() {
    return name.charAt(0);
  }


  @Override
  protected final Object delegateObj() {
    return name;
  }
  
  
  
  /**
   * Returns a singleton group (that also picks up this option in abbreviated
   * sym form).
   */
  public OptionGroup group() {
    return new OptionGroup(this);
  }

  
  /**
   * Tests whether the given string equals the {@linkplain #getFull() full} option.
   * 
   * @return {@code getFull().equals(value)}
   */
  @Override
  public boolean test(String value) {
    int len = value == null ? 0 : value.length();
    if (len < 2 || value.charAt(0) != '-')
      return false;
    if (len == 2)
      return value.charAt(1) == getSym();
    return
        len == name.length() + 2 &&
        value.charAt(1) == '-' &&
        name.equals(value.substring(2));
  }
  
  
  /**
   * Removes this option if present from the given {@code args} and returns
   * the result.
   * 
   * @return {@code true} if present in the given arg list
   * @throws IllegalArgumentException if present multiple times
   * 
   * @see #removeOption(ArgList)
   */
  public boolean remove(ArgList args) throws IllegalArgumentException {
    var matched = args.removeMatched(this);
    switch (matched.size()) {
    case 0:   return false;
    case 1:   return true;
    default:  throw new IllegalArgumentException("duplicate options: " + matched);
    }
  }
  

  /**
   * Removes this option if present from the given {@code args} and returns
   * the result.
   * 
   * @return a read-only set: singleton containing this instance, if present; empty, o.w.
   * @throws IllegalArgumentException if present multiple times
   * 
   * @see #remove(ArgList)
   */
  public Set<Option> removeOption(ArgList args) throws IllegalArgumentException {
    return remove(args) ? Collections.singleton(this) : Collections.emptySet();
  }
  
  
  /**
   * Returns {@linkplain #getFull()}.
   */
  @Override
  public String toString() {
    return getFull();
  }

}
