/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.main;


import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import io.crums.util.Lists;

/**
 * Groups {@linkplain Option option}s so that their symbol shortcuts are
 * also interpreted when grouped.
 * 
 * @see #removeOptions(ArgList)
 */
public class OptionGroup implements Predicate<String> {
  
  /**
   * Removes and returns matching {@code options} in the {@code argList}.
   * 
   * @throws IllegalArgumentException if the {@code options} collide (at
   *         the first character).
   */
  public static Set<Option> remove(ArgList args, Option... options) {
    return new OptionGroup(options).removeOptions(args);
  }
  
  private final List<Option> options;
  
  private final ShortOptions shortOpts;
  
  
  /**
   * Creates an instance with the given full option names. Note their
   * short form symbols must not collide.
   */
  public OptionGroup(String... optNames) {
    this(Lists.map(optNames, name -> new Option(name)));
  }
  
  
  /**
   * Creates an instance. Note their
   * short form symbols must not collide.
   * 
   * @param options no duplicate first characters
   */
  public OptionGroup(Option... options) {
    this(List.of(options));
  }
  

  /**
   * Creates an instance. Note their
   * short form symbols must not collide.
   * 
   * @param options not empty, no duplicate first characters
   */
  public OptionGroup(List<Option> options) {
    this.options = options;
    if (options.isEmpty())
      throw new IllegalArgumentException("empty options list");
    if (new HashSet<>(options).size() != options.size())
      throw new IllegalArgumentException("duplicate options: " + options);
    var symbols = new StringBuilder();
    this.options.forEach(o -> symbols.append(o.getSym()));
    this.shortOpts = new ShortOptions(symbols.toString());
  }
  
  
  

  /**
   * Tests whether the given string is any of the option settings or
   * combination of their abbreviated symbols.
   * 
   * @see #removeOptions(ArgList)
   */
  @Override
  public boolean test(String value) {
    if (value.length() < 2 || value.charAt(0) != '-')
      return false;
    if (value.charAt(1) == '-') {
      String optName = value.substring(2);
      for (var opt : options)
        if (opt.getName().equals(optName))
          return true;
      return false;
    }
    return shortOpts.test(value);
  }
  
  
  
  /**
   * Removes and returns options from the given {@code args}. Both fully
   * named options (eg {@code --MyOpt --moOpt}) and abbreviated / combined
   * patterns (eg {@code -mM}) are picked up.
   */
  public Set<Option> removeOptions(ArgList args) {
    List<String> matched = args.removeMatched(this);
    if (matched.isEmpty())
      return Collections.emptySet();
    
    var out = new HashSet<Option>();
    int count = 0;
    for (String arg : matched) {
      if (arg.startsWith("--")) {
        out.add(new Option(arg.substring(2)));
        ++count;
      } else {
        var syms = arg.substring(1);
        for (int index = syms.length(); index-- > 0; )
          out.add(forSym(syms.charAt(index)));
        count += syms.length();
      }
    }
    if (count != out.size())
      throw new IllegalArgumentException("duplicate options: " + matched);
    
    return out;
  }
  
  
  private Option forSym(char sym) {
    for (var option : options)
      if (option.getSym() == sym)
        return option;
    throw new RuntimeException("assertion failed with sym '" + sym + "'");
  }

}
