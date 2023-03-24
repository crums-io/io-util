/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Utility methods for lists. These are assume lists are <em>always</em> {@linkplain RandomAccess random access}.
 * To my mind, linked lists (despite having "list" in their name) don't belong to the interface.
 */
public class Lists {

  private Lists() { }
  
  
  @SuppressWarnings("rawtypes")
  private final static List SINK =
      new RandomAccessList<>() {

        @Override
        public boolean add(Object e) {
          return true;
        }

        @Override
        public void add(int index, Object element) {   }

        @Override
        public boolean addAll(int index, Collection<? extends Object> c) {
          return true;
        }

        @Override
        public Object get(int index) {
          throw new IndexOutOfBoundsException(index);
        }
    
        @Override
        public int size() {
          return 0;
        }
      };
  
  
  /**
   * Returns a list whose <em>add</em> methods don't do anything. It breaks
   * the interface, but if you have a method that takes a list as a collector,
   * then this might sometimes be useful. Note, this is meant for use <em>inside</em>
   * a class definition that knows how the list is used; it's not meant to be
   * exposed to users.
   * 
   * @return an ever-empty list that silently drops whatever is added to it.
   *         It's actually a singleton instance.
   */
  @SuppressWarnings("unchecked")
  public static <T> List<T> sink() {
    return (List<T>) SINK;
  }
  
  /**
   * Returns a read-only view.
   */
  public static <U, V> List<V> map(List<U> source, Function<U, V> mapper) {
    return source.isEmpty() ?
        Collections.emptyList() :
          new MappedView<U, V>(source, mapper);
  }
  
  /**
   * Returns a read-only view.
   */
  public static <U, V> List<V> map(U[] source, Function<U, V> mapper) {
    return
        source.length == 0 ?
            Collections.emptyList() :
              map(asReadOnlyList(source), mapper);
  }
  
  
  public static <U, V> List<V> mapByIndex(List<U> source, BiFunction<Integer, U, V> mapper) {
    return source.isEmpty() ?
        Collections.emptyList() :
          new MappedIndexView<>(source, mapper);
  }
  
  
  /**
   * Returns a read-write view.
   */
  public static <U, V> List<V> map(List<U> source, Isomorphism<U, V> iso) {
    return new IsomorphicView<>(source, iso);
  }
  
  
  /**
   * Returns a read-only view of the given elements. The intended usage is an
   * <em>unmodified</em> array: it is used as is and does not check for null elements.
   * As such, for large arrays, it is significantly faster than the now standard
   * {@linkplain List#of(Object...)} implementation.
   * 
   * @param element non-null array of non-null elements
   * @return
   */
  public static <T> List<T> asReadOnlyList(@SuppressWarnings("unchecked") T... element) {
    switch (element.length) {
    case 0 :  return List.of();
    case 1 :  return List.of(element[0]);
    case 2 :  return List.of(element[0], element[1]);
    case 3 :  return List.of(element[0], element[1], element[2]);
    default:  return new ArrayView<>(element);
    }
  }
  
  
  /**
   * Returns a read-only version of the given list. If the size of the given
   * list is less than 4, returns an immutable copy; if 4 or more, it's only
   * a read-only view.
   * <p>
   * Motivation for this method is an opportunity to centralize how this can
   * be done most efficiently (should {@code List.of(..)} be mixed in with
   * {{@code Collections.unmodifiableList}, for eg?). TODO: gather/find data
   * for this.
   * </p>
   * 
   * @param copy
   * @return
   */
  public static <T> List<T> asReadOnlyList(List<T> copy) {
    switch (copy.size()) {
    case 0:   return List.of();
    case 1:   return List.of(copy.get(0));
    case 2:   return List.of(copy.get(0), copy.get(1));
    case 3:   return List.of(copy.get(0), copy.get(1), copy.get(2));
    default:  return Collections.unmodifiableList(copy);
    }
  }
  
  
  
  /**
   * Returns a read-only copy of the argument. Used mostly with constructor
   * arguments of immutable classes.
   * 
   * @param copy non-null, with non null values; empty OK
   */
  public static <T> List<T> readOnlyCopy(Collection<? extends T> copy) {
    return readOnlyCopy(copy, false);
  }
  

  /**
   * Returns a read-only copy of the argument. Used mostly with constructor
   * arguments of immutable classes.
   * 
   * <h4>Duplicates semantics</h4>
   * <p>
   * These semantics are relevant only if invoked with {@code noDups} set to {@code true}.
   * </p><p>
   * We know nothing about type {@code <T>}, so equality (and hence duplicates)
   * semantics is governed by {@code Object.equals(Object)} in the usual way.
   * {@code <T>} must also implement {@code Object.hashCode()} per the general contract.
   * </p>
   * 
   * @param copy non-null, with non null values; empty OK
   * @param noDups if {@code true}, then the values are checked not to have duplicates
   */
  public static <T> List<T> readOnlyCopy(Collection<? extends T> copy, boolean noDups) {
    
    final int size = Objects.requireNonNull(copy, "null list").size();
    switch (size) {
    case 0:
      return List.of();
    case 1:
      return List.of(copy.iterator().next());
    case 2:
      if (noDups)
        break;
      T a, b;
      {
        var iter = copy.iterator();
        a = iter.next();
        b = iter.next();
      }
      return List.of(a, b);
    case 3:
      if (noDups)
        break;
      T c;
      {
        var iter = copy.iterator();
        a = iter.next();
        b = iter.next();
        c = iter.next();
      }
      return List.of(a, b, c);
    case 4:
      if (noDups)
        break;
      T d;
      {
        var iter = copy.iterator();
        a = iter.next();
        b = iter.next();
        c = iter.next();
        d = iter.next();
      }
      return List.of(a, b, c, d);
    }
    
    if (!noDups)
      return List.copyOf(copy);
    
    ArrayList<T> out = new ArrayList<>(size);
    HashSet<T> set = new HashSet<>(size);
    
    for (var iter = copy.iterator(); iter.hasNext();) {
      var next = iter.next();
      // disallow null
      if (next == null)
        throw new IllegalArgumentException("argument has null elements: " + copy);
      if (!set.add(next))
        throw new IllegalArgumentException(
            "argument contains duplicate (" + next + "): " + copy);
      out.add(next);
    }
    return Collections.unmodifiableList(out);
  }
  
  
  /**
   * Returns a sorted, read-only list from the given collection, optionally <em>checking</em>
   * that it hss no duplicates.
   * 
   * @param <T> naturally comparable type
   * @param noDups no duplicates allowed if {@code true} (more expensive)
   * 
   * @see #sortRemoveDups(Collection)
   * @throws IllegalArgumentException if {@code noDups} is {@code true} and {@code copy} contains duplicates
   */
  public static <T extends Comparable<T>> List<T> sort(Collection<? extends T> copy, boolean noDups) {
    int size = Objects.requireNonNull(copy).size();
    switch (size) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(copy.iterator().next());
    }
    List<T> out = new ArrayList<>(copy.size());
    if (noDups) {
      var set = new TreeSet<>(copy);
      if (set.size() != copy.size())
        throw new IllegalArgumentException("argument has duplicates");
      out.addAll(set);
    } else {
      out.addAll(copy);
      Collections.sort(out);
    }
    return Collections.unmodifiableList(out);
  }

  
  
  /**
   * Returns the contents of the given collection as a sorted, read-only list with
   * no duplicates.
   * 
   * @param <T> naturally comparable type
   * @param copy  may be empty or with duplicates
   * 
   * @see #sort(Collection, boolean)
   */
  public static <T extends Comparable<T>> List<T> sortRemoveDups(Collection<? extends T> copy) {
    int size = Objects.requireNonNull(copy).size();
    switch (size) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(copy.iterator().next());
    }
    return readOnlyCopy(new TreeSet<T>(copy));
  }
  
  
  
  
  /**
   * Returns a read-only copy of the naturally ascending list containing no
   * duplicate elements.
   * 
   * @param <T> naturally comparable type
   * @param copy ordered list, monotonically ascending
   * 
   * @return a read-only copy
   */
  public static <T extends Comparable<T>> List<T> readOnlyOrderedCopy(List<T> copy) {
    return readOnlyOrderedCopy(copy, true);
  }
  
  
  
  /**
   * Returns a read-only copy of the naturally ascending list.
   * This doesn't sort the list: it validates that it is indeed sorted.
   * 
   * @param <T> naturally comparable type
   * @param copy ordered list
   * @param noDups if {@code true}, then {@code copy} must be monotonically ascending
   * 
   * @return a read-only copy
   */
  public static <T extends Comparable<T>> List<T> readOnlyOrderedCopy(List<T> copy, boolean noDups) {
    
    final int size = Objects.requireNonNull(copy, "null list").size();
    switch (size) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(copy.get(0));
    }

    ArrayList<T> out = new ArrayList<>(size);
    T prev = copy.get(0);
    out.add(prev);
    
    for (int index = 1; index < size; ++index) {
      T next = copy.get(index);
      
      int comp = next.compareTo(prev);
      if (comp <= 0) {
        if (comp == 0) {
          if (noDups)
            throw new IllegalArgumentException(
                "duplicate (" + next + ") at index " + index + " in list " + copy);
        } else {
          throw new IllegalArgumentException(
              "out-of-order elements at index " + index + "(" + prev + " and " + next +
              ") in list " + copy);
        }
      }
      prev = next;
      
      out.add(next);
    }
    return Collections.unmodifiableList(out);
  }
  
  
  /**
   * Determines whether the given {@code list} is sorted and contains no duplicates.
   * 
   * @param <T> naturally comparable type
   * @param list
   * 
   * @return {@code isSorted(list, false)}
   */
  public static <T extends Comparable<T>> boolean isSortedNoDups(List<T> list) {
    return isSorted(list, false);
  }
  
  /**
   * Determines whether the given {@code list} is sorted.
   * 
   * 
   * @param <T> naturally comparable type
   * @param dupsOk if {@code false}, then duplicate elements will fail (return false)
   */
  public static <T extends Comparable<T>> boolean isSorted(List<T> list, boolean dupsOk) {
    final int size = list.size();
    if (size < 2)
      return true;
    
    T prev = list.get(0);
    final int compMax = dupsOk ? 1 : 0; // exclusive
    
    for (int index = 1; index < size; ++index) {
      T next = list.get(index);
      if (next.compareTo(prev) < compMax)
        return false;
      prev = next;
    }
    return true;
  }
  
  
  /**
   * Returns a reversed, read-only view of the given source list.
   * If the <code>source</code> is a singleton or empty, it is returned as-is.
   * <p>
   * Note, reversing an already reversed list <em>unwraps</em> the view.
   * (You needn't worry about needless double-wrappings.)
   * </p>
   */
  public static <T> List<T> reverse(List<T> source) {
    if (source.size() < 2)
      return source;
    return source instanceof ReverseView<T> rev ? rev.source() : new ReverseView<>(source);
  }
  
  
  public static <T> List<T> concat(List<T> head, List<T> tail) {
    if (head.isEmpty())
      return tail;
    else if (tail.isEmpty())
      return head;
    else
      return new ConcatList<>(head, tail);
  }
  
  
  
  public static <T> List<T> concat(T head, List<T> list) {
    return concatLists(List.of(head), list);
  }
  
  public static <T> List<T> concat(List<T> list, T tail) {
    return concatLists(list, List.of(tail));
  }
  
  
  
  public static <T> List<T> concat(T head, List<T> list, T tail) {
    return concatLists(List.of(head), list, List.of(tail));
  }
  
  
  
  @SafeVarargs
  public static <T> List<T> concatLists(List<T>... lists) {
    var notEmpties = Arrays.asList(lists).stream().filter(list -> !list.isEmpty()).toList();
    switch (notEmpties.size()) {
    case 0:   return List.of();
    case 1:   return notEmpties.get(0);
    case 2:   return new ConcatList<>(notEmpties.get(0), notEmpties.get(1));
    default:  return new MultiCatList<>(notEmpties);
    }
  }
  
  
  
  /**
   * Returns a read-only view of the given {@code array}.
   */
  public static List<Integer> intList(int[] array) {
    Objects.requireNonNull(array, "null array");
    return new RandomAccessList<Integer>() {
      @Override
      public Integer get(int index) {
        return array[index];
      }
      @Override
      public int size() {
        return array.length;
      }
    };
  }
  
  

  /**
   * Returns a contiguous list of {@code int}s in the range {@code [a, b]}.
   * Consumes little (constant) memory no matter the size of the range.
   * 
   * @param a the highest or lowest number in the range (inclusive)
   * @param b the highest or lowest number in the range (inclusive)
   * 
   * @return non-empty, read-only list
   * @throws IllegalArgumentException if the size of the given range is greater
   *         than {@linkplain Integer#MAX_VALUE}
   */
  public static List<Integer> intRange(int a, int b) {
    final int lo;
    int diff;
    if (a < b) {
      lo = a;
      diff = b - a;
    } else if (a == b) {
      return Collections.singletonList(a);
    } else {
      lo = b;
      diff = a - b;
    }
    if (diff < 0 || diff == Integer.MAX_VALUE)
      throw new IllegalArgumentException("range overflow (" + a + "," + b + ")");
    
    final int size = diff + 1;
    
    return new RandomAccessList<Integer>() {
      @Override
      public Integer get(int index) {
        Objects.checkIndex(index, size);
        return lo + index;
      }
      @Override
      public int size() {
        return size;
      }
    };
  }
  

  /**
   * Returns a read-only view of the given {@code array}.
   */
  public static List<Long> longList(long[] array) {
    Objects.requireNonNull(array, "null array");
    return new RandomAccessList<Long>() {
      @Override
      public Long get(int index) {
        return array[index];
      }
      @Override
      public int size() {
        return array.length;
      }
    };
  }
  
  
  /**
   * Returns a contiguous list of ascending {@code long}s in the range {@code [a, b]},
   * inclusive. Consumes constant memory no matter the size of the range.
   * 
   * @param a the highest or lowest number in the range (inclusive)
   * @param b the highest or lowest number in the range (inclusive)
   * 
   * @return non-empty, read-only list
   * @throws IllegalArgumentException if the size of the given range is greater
   *         than {@linkplain Integer#MAX_VALUE}
   */
  public static List<Long> longRange(long a, long b) {
    final long lo;
    long diff;
    if (a < b) {
      lo = a;
      diff = b - a;
    } else if (a == b) {
      return Collections.singletonList(a);
    } else {
      lo = b;
      diff = a - b;
    }
      
    if (diff < 0 || diff >= Integer.MAX_VALUE)
      throw new IllegalArgumentException("range overflow (" + a + "," + b + ")");
    
    final int isize = (int) diff + 1;
    
    return new RandomAccessList<Long>() {
      @Override
      public Long get(int index) {
        Objects.checkIndex(index, isize);
        return lo + index;
      }
      @Override
      public int size() {
        return isize;
      }
    };
  }
  
  
  /** Returns a read-only view of the given {@code array}. */
  public static List<Double> doubleList(double[] array) {
    Objects.requireNonNull(array, "null array");
    return new RandomAccessList<Double>() {
      @Override
      public Double get(int index) {
        return array[index];
      }
      @Override
      public int size() {
        return array.length;
      }
    };
  }
  
  
  /** Returns a read-only view of the given {@code array}. */
  public static List<Float> floatList(float[] array) {
    Objects.requireNonNull(array, "null array");
    return new RandomAccessList<Float>() {
      @Override
      public Float get(int index) {
        return array[index];
      }
      @Override
      public int size() {
        return array.length;
      }
    };
  }
  
  
  
  public static <T> List<T> repeatedList(T repeated, int size) {
    Objects.requireNonNull(repeated);
    if (size < 2) {
      switch (size) {
      case 0: return Collections.emptyList();
      case 1: return Collections.singletonList(repeated);
      default:
        throw new IllegalArgumentException("size: " + size);
      }
    }
    return new RandomAccessList<T>() {
      @Override
      public T get(int index) {
        Objects.checkIndex(index, size);
        return repeated;
      }
      @Override
      public int size() {
        return size;
      }
    };
  }
  
  
  /**
   * Returns a list view of the given access function. Note successive invocations
   * of {@linkplain List#get(int) get(index)} on the same index return different
   * objects.
   * 
   * @param size the fixed size of the list
   * @param accessor
   * @return a lazy, lightweight, read-only list of {@code accesssor}
   */
  public static <T> List<T> functorList(int size, Function<Integer, T> accessor) {
    return new FunctorList<>(size, accessor);
  }
  
  
  
  /**
   * Returns a <em>downcast</em> view of the given list. This is an efficient workaround
   * for the inability to do casts of the following type in Java: {@code (List<CharSequence>) List<String>}.
   * The objects in the list themselves are not transformed.
   * <p>
   * <em>Note the transformed view never throws {@linkplain ClassCastException}s.</em>
   * </p>
   * 
   * @param <D> the downcast type
   * @param <U> the original type (a subtype of {@code <D>}
   * 
   * @return {@code map(list, u -> (D) u)}
   */
  public static <D, U extends D> List<D> downcast(List<U> list) {
    return map(list, u -> (D) u);
  }
  
  

  /**
   * Returns a <em>upcast</em> view of the given list. This is an efficient workaround
   * for the inability to do casts of the following type in Java: {@code (List<String>) List<CharSequence>}.
   * The objects in the list themselves are not transformed.
   * <p>
   * Note this method does not inspect the elements of the given list: <em>a {@linkplain ClassCastException}
   * may later be thrown on accessing an element that is not castable.</em>
   * </p>
   * 
   * @param <D> the original type
   * @param <U> the type cast to (a subtype of {@code <D>}
   * 
   * @return {@code map(list, u -> (D) u)}
   */
  @SuppressWarnings("unchecked")
  public static <D, U extends D> List<U> upcast(List<D> list) {
    return map(list, d -> (U) d);
  }
  
  
  
  
  
  
  /**
   * Extend <em>this</em> class instead of {@linkplain AbstractList}. (Really, a list that's not random access is
   * not a list should just be called a collection.)
   */
  public static abstract class RandomAccessList<T> extends AbstractList<T> implements RandomAccess {
    
  }
  
  
  /**
   * An index-to-object function wrapped as a read-only list.
   */
  public static class FunctorList<T> extends RandomAccessList<T> {
    
    private final Function<Integer, T> accessor;
    private final int size;
    
    
    public FunctorList(int size, Function<Integer, T> accessor) {
      this.size = size;
      this.accessor = Objects.requireNonNull(accessor, "null accessor function");
      if (size < 0)
        throw new IllegalArgumentException("negative size: " + size);
    }

    @Override
    public T get(int index) {
      Objects.checkIndex(index, size);
      return accessor.apply(index);
    }

    @Override
    public int size() {
      return size;
    }
    
  }
  
  
  /**
   * A concatenated view of 2 lists.
   */
  public static class ConcatList<T> extends RandomAccessList<T> {
    
    private final List<T> head;
    private final List<T> tail;
    
    /**
     * Creates a new instance with the given <code>first</code> and <code>second</code>
     * sublists.
     * 
     * @param head
     * @param tail
     * 
     */
    public ConcatList(List<T> head, List<T> tail) {
      this.head = Objects.requireNonNull(head, "null head");
      this.tail = Objects.requireNonNull(tail, "null tail");
    }

    @Override
    public T get(int index) {
      if (index < 0)
        throw new IndexOutOfBoundsException(index);
      
      int firstSize = head.size();
      
      if (index < firstSize)
        return head.get(index);
      
      if (index < firstSize + tail.size())
        return tail.get(index - firstSize);
      
      throw new IndexOutOfBoundsException(index);
    }

    @Override
    public int size() {
      return head.size() + tail.size();
    }
    
    /**
     * Returns the head (lower index) sublist.
     */
    public List<T> headList() {
      return head;
    }
    
   /**
    * Returns the tail (higher index) sublist.
    */
    public List<T> tailList() {
      return tail;
    }
  }
  
  
  
  
  public static class MultiCatList<T> extends RandomAccessList<T> {
    
    private final int BINARY_SEARCH_SIZE_THRESHOLD = 32;
    
    private final Object[] lists;
    private final int[] cumulativeSizes;
    private final boolean binarySearchIndex;
    
    public MultiCatList(List<List<T>> lists) {
      this(lists, true, false);
    }
    
    public MultiCatList(List<List<T>> lists, boolean binarySearchIndex) {
      this(lists, false, binarySearchIndex);
    }
    
    private MultiCatList(List<List<T>> lists, boolean auto, boolean binarySearchIndex) {
      Objects.requireNonNull(lists, "null lists");
      this.lists = lists.toArray();
      
      final int count = lists.size();
      if (count < 2)
        throw new IllegalArgumentException("too few lists (" + count + ")");
      
      this.cumulativeSizes = new int[count];
      int size = 0;
      for (int index = 0; index < count; ++index) {
        var list = lists.get(index);
        if (list == null)
          throw new NullPointerException("list [" + index + "] is null");
        int subSize = list.size();
        if (subSize == 0)
          throw new IllegalArgumentException("empty list [" + index + "]");
        size += subSize;
        cumulativeSizes[index] = size;
      }
      
      this.binarySearchIndex = auto ? (count > BINARY_SEARCH_SIZE_THRESHOLD) : binarySearchIndex;
    }
    
    @SuppressWarnings("unchecked")
    private List<T> subList(int index) {
      return (List<T>) lists[index];
    }
    
    
    
    

    @Override
    public T get(int index) {
      Objects.checkIndex(index, size());
      int listIndex = listIndex(index);
      var subList = subList(listIndex);
      int headSize = listIndex == 0 ? 0 : cumulativeSizes[listIndex - 1];
      
      return subList.get(index - headSize);
    }
    
    private int listIndex(int index) {
      if (binarySearchIndex) {
        int si = Arrays.binarySearch(cumulativeSizes, index);
        return si < 0 ? -1 - si : si + 1;
      } else {
        int ic = cumulativeSizes.length - 1;
        while (ic-- > 0 && cumulativeSizes[ic] > index);
        return ic + 1;
      }
    }

    @Override
    public final int size() {
      return cumulativeSizes[cumulativeSizes.length - 1];
    }
  }
  
  
  
  
  static class ReverseView<T> extends RandomAccessList<T> {
    
    private final List<T> source;
    
    protected ReverseView(List<T> source) {
      this.source = Objects.requireNonNull(source);
    }

    @Override
    public T get(int index) {
      int size = size();
      Objects.checkIndex(index, size);
      return source.get(size - 1 - index);
    }

    @Override
    public int size() {
      return source.size();
    }
    
    
    public List<T> source() {
      return source;
    }
  }
  
  
  static class ArrayView<T> extends RandomAccessList<T> {
    
    protected final T[] array;
    
    protected ArrayView(T[] array) {
      this.array = Objects.requireNonNull(array);
    }

    @Override
    public T get(int index) {
      return array[index];
    }

    @Override
    public int size() {
      return array.length;
    }
  }
  

  
  static abstract class BaseView<U,V> extends RandomAccessList<V> {
    
    protected final List<U> source;
    
    protected BaseView(List<U> source) {
      this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public int size() {
      return source.size();
    }
    
  }
  
  
  
  static class MappedIndexView<U, V> extends BaseView<U, V> {
    
    private final BiFunction<Integer, U, V> mapper;
    
    
    public MappedIndexView(List<U> source, BiFunction<Integer, U, V> mapper) {
      super(source);
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public V get(int index) {
      return mapper.apply(index, source.get(index));
    }
    
  }
  
  
  
  static class MappedView<U, V> extends BaseView<U, V> {
    
    private final Function<U, V> mapper;
    
    protected MappedView(List<U> source, Function<U, V> mapper) {
      super(source);
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public V get(int index) {
      return mapper.apply(source.get(index));
    }
    
  }
  
  
  
  
  
  static class IsomorphicView<U, V> extends BaseView<U, V> {
    
    private final Isomorphism<U, V> iso;
    
    protected IsomorphicView(List<U> source, Isomorphism<U, V> iso) {
      super(source);
      this.iso = Objects.requireNonNull(iso, "iso");
    }
    
    

    @Override
    public boolean add(V e) {
      return source.add(iso.inverse().apply(e));
    }



    @Override
    public V get(int index) {
      return iso.mapping().apply(source.get(index));
    }
    
  }
  
  /**
   * Zips the 2 given lists into a lazily merged resultant list.
   * The size of the returned zipped list is that of the <em>left</em> list.
   * 
   * @param <T> type of returned list
   * @param <U> type of left list
   * @param <V> type of right list
   * @param left      not null
   * @param right     not null and of size &ge; {@code left.size()}
   * @param mergeFunc merge function {@code (U,V) -> T} invoked every time
   *                  an element is accessed
   * 
   * @return non-null, read-only list
   */
  public static <T, U, V> List<T> zip(List<U> left, List<V> right, BiFunction<U, V, T> mergeFunc) {
    if (left.isEmpty()) {
      checkZipArgs(left, right, mergeFunc);
      return List.of();
    }
    return new ZipList<>(left, right, mergeFunc);
  }
  
  
  private static <U, V> void checkZipArgs(List<U> left, List<V> right, BiFunction<U, V, ?> mergeFunc) {
    if (left.size() > right.size())
      throw new IllegalArgumentException(
          "left size (%d) > right size (%d)"
          .formatted(left.size(), right.size()));
    Objects.requireNonNull(mergeFunc, "null merge function");
  }
  
  
  public static class ZipList<T,U,V> extends RandomAccessList<T> {
    
    private final List<U> left;
    private final List<V> right;
    
    private final BiFunction<U, V, T> mergeFunc;
    
    
    /**
     * Constructs an list using 2 lists and a merge function. The size of the
     * list is equal to that of the <em>left</em> list.
     * 
     * @param left
     * @param right
     * @param mergeFunc
     */
    public ZipList(List<U> left, List<V> right, BiFunction<U, V, T> mergeFunc) {
      
      this.left = left;
      this.right = right;
      this.mergeFunc = mergeFunc;
      
      checkZipArgs(left, right, mergeFunc);
    }

    @Override
    public T get(int index) {
      return mergeFunc.apply(left.get(index), right.get(index));
    }

    @Override
    public int size() {
      return left.size();
    }
    
  }
  
  

}
