/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
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
          new ReadOnlyView<U, V>(source, mapper);
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
  
  
  /**
   * Returns a read-write view.
   */
  public static <U, V> List<V> map(List<U> source, Isomorphism<U, V> iso) {
    return new IsomorphicView<>(source, iso);
  }
  
  
  public static <T> List<T> asReadOnlyList(@SuppressWarnings("unchecked") T... element) {
    switch (element.length) {
    case 0 :  return Collections.emptyList();
    case 1 :  return Collections.singletonList(element[0]);
    default:  return new ArrayView<>(element);
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
   * <h3>Duplicates semantics</h3>
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
    
    int size = Objects.requireNonNull(copy, "null list").size();
    switch (size) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(copy.iterator().next());
    }
    
    ArrayList<T> out = new ArrayList<>(size);
    
    if (noDups) {
      HashSet<T> set = new HashSet<>(size);
      set.addAll(copy);
      if (set.size() != size)
        throw new IllegalArgumentException("argument contains duplicates: " + copy);
    }
    for (var iter = copy.iterator(); iter.hasNext();) {
      var next = iter.next();
      // disallow null
      if (next == null)
        throw new IllegalArgumentException("argument has null elements: " + copy);
      out.add(next);
    }
    return Collections.unmodifiableList(out);
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
   * If the <tt>source</tt> is a singleton or empty, it is returned as-is.
   */
  public static <T> List<T> reverse(List<T> source) {
    
    return source.size() <= 1 ? source : new ReverseView<>(source);
  }
  
  
  public static <T> List<T> concat(List<T> head, List<T> tail) {
    if (head.isEmpty())
      return tail;
    else if (tail.isEmpty())
      return head;
    else
      return new ConcatList<>(head, tail);
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
  
  
  /**
   * Extend <em>this</em> class instead of {@linkplain AbstractList}. (Really, a list that's not random access is
   * not a list should just be called a collection.)
   */
  public static abstract class RandomAccessList<T> extends AbstractList<T> implements RandomAccess {
    
  }
  
  
  /**
   * A concatenated view of 2 lists.
   */
  public static class ConcatList<T> extends RandomAccessList<T> {
    
    private final List<T> head;
    private final List<T> tail;
    
    /**
     * Creates a new instance with the given <tt>first</tt> and <tt>second</tt>
     * sublists.
     * 
     * @param head
     * @param tail
     * 
     * @see #firstSubList()
     * @see #secondSubList()
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
  
  
  
  
  protected static class ReverseView<T> extends RandomAccessList<T> {
    
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
  }
  
  
  protected static class ArrayView<T> extends RandomAccessList<T> {
    
    private final T[] array;
    
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
  
  
  
  protected static class ReadOnlyView<U, V> extends BaseView<U, V> {
    
    private final Function<U, V> mapper;
    
    protected ReadOnlyView(List<U> source, Function<U, V> mapper) {
      super(source);
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public V get(int index) {
      return mapper.apply(source.get(index));
    }
    
  }
  
  
  protected static class IsomorphicView<U, V> extends BaseView<U, V> {
    
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
  
  

}
