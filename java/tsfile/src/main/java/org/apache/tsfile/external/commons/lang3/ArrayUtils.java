/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.external.commons.lang3;

import java.lang.reflect.Array;
import java.util.function.Supplier;

public class ArrayUtils {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /** An empty immutable {@code boolean} array. */
  public static final boolean[] EMPTY_BOOLEAN_ARRAY = {};

  /** An empty immutable {@code float} array. */
  public static final float[] EMPTY_FLOAT_ARRAY = {};

  /** An empty immutable {@code int} array. */
  public static final int[] EMPTY_INT_ARRAY = {};

  /** An empty immutable {@code long} array. */
  public static final long[] EMPTY_LONG_ARRAY = {};

  public static final String[] EMPTY_STRING_ARRAY = {};

  /** An empty immutable {@code double} array. */
  public static final double[] EMPTY_DOUBLE_ARRAY = {};

  /**
   * Converts an array of object Longs to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param array a {@link Long} array, may be {@code null}
   * @return a {@code long} array, {@code null} if null array input
   * @throws NullPointerException if an array element is {@code null}
   */
  public static long[] toPrimitive(final Long[] array) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return EMPTY_LONG_ARRAY;
    }
    final long[] result = new long[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i];
    }
    return result;
  }

  /**
   * Removes the element at the specified position from the specified array. All subsequent elements
   * are shifted to the left (subtracts one from their indices).
   *
   * <p>This method returns a new array with the same elements of the input array except the element
   * on the specified position. The component type of the returned array is always the same as that
   * of the input array.
   *
   * <p>If the input array is {@code null}, an IndexOutOfBoundsException will be thrown, because in
   * that case no valid index can be specified.
   *
   * <pre>
   * ArrayUtils.remove(["a"], 0)           = []
   * ArrayUtils.remove(["a", "b"], 0)      = ["b"]
   * ArrayUtils.remove(["a", "b"], 1)      = ["a"]
   * ArrayUtils.remove(["a", "b", "c"], 1) = ["a", "c"]
   * </pre>
   *
   * @param <T> the component type of the array
   * @param array the array to remove the element from, may not be {@code null}
   * @param index the position of the element to be removed
   * @return A new array containing the existing elements except the element at the specified
   *     position.
   * @throws IndexOutOfBoundsException if the index is out of range (index &lt; 0 || index &gt;=
   *     array.length), or if the array is {@code null}.
   * @since 2.1
   */
  @SuppressWarnings("unchecked") // remove() always creates an array of the same type as its input
  public static <T> T[] remove(final T[] array, final int index) {
    return (T[]) remove((Object) array, index);
  }

  /**
   * Removes the element at the specified position from the specified array. All subsequent elements
   * are shifted to the left (subtracts one from their indices).
   *
   * <p>This method returns a new array with the same elements of the input array except the element
   * on the specified position. The component type of the returned array is always the same as that
   * of the input array.
   *
   * <p>If the input array is {@code null}, an IndexOutOfBoundsException will be thrown, because in
   * that case no valid index can be specified.
   *
   * @param array the array to remove the element from, may not be {@code null}
   * @param index the position of the element to be removed
   * @return A new array containing the existing elements except the element at the specified
   *     position.
   * @throws IndexOutOfBoundsException if the index is out of range (index &lt; 0 || index &gt;=
   *     array.length), or if the array is {@code null}.
   * @since 2.1
   */
  private static Object remove(final Object array, final int index) {
    final int length = getLength(array);
    if (index < 0 || index >= length) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Length: " + length);
    }
    final Object result = Array.newInstance(array.getClass().getComponentType(), length - 1);
    System.arraycopy(array, 0, result, 0, index);
    if (index < length - 1) {
      System.arraycopy(array, index + 1, result, index, length - index - 1);
    }
    return result;
  }

  /**
   * Gets the length of the specified array. This method can deal with {@link Object} arrays and
   * with primitive arrays.
   *
   * <p>If the input array is {@code null}, {@code 0} is returned.
   *
   * <pre>
   * ArrayUtils.getLength(null)            = 0
   * ArrayUtils.getLength([])              = 0
   * ArrayUtils.getLength([null])          = 1
   * ArrayUtils.getLength([true, false])   = 2
   * ArrayUtils.getLength([1, 2, 3])       = 3
   * ArrayUtils.getLength(["a", "b", "c"]) = 3
   * </pre>
   *
   * @param array the array to retrieve the length from, may be {@code null}.
   * @return The length of the array, or {@code 0} if the array is {@code null}
   * @throws IllegalArgumentException if the object argument is not an array.
   * @since 2.1
   */
  public static int getLength(final Object array) {
    return array != null ? Array.getLength(array) : 0;
  }

  /**
   * Reverses the order of the given array.
   *
   * <p>This method does nothing for a {@code null} input array.
   *
   * @param array the array to reverse, may be {@code null}.
   */
  public static void reverse(final int[] array) {
    if (array != null) {
      reverse(array, 0, array.length);
    }
  }

  /**
   * Reverses the order of the given array in the given range.
   *
   * <p>This method does nothing for a {@code null} input array.
   *
   * @param array the array to reverse, may be {@code null}.
   * @param startIndexInclusive the starting index. Undervalue (&lt;0) is promoted to 0, overvalue
   *     (&gt;array.length) results in no change.
   * @param endIndexExclusive elements up to endIndex-1 are reversed in the array. Undervalue (&lt;
   *     start index) results in no change. Overvalue (&gt;array.length) is demoted to array length.
   * @since 3.2
   */
  public static void reverse(
      final int[] array, final int startIndexInclusive, final int endIndexExclusive) {
    if (array == null) {
      return;
    }
    int i = Math.max(startIndexInclusive, 0);
    int j = Math.min(array.length, endIndexExclusive) - 1;
    int tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  /**
   * Tests whether an array of Objects is empty or {@code null}.
   *
   * @param array the array to test
   * @return {@code true} if the array is empty or {@code null}
   * @since 2.1
   */
  public static boolean isEmpty(final Object[] array) {
    return isArrayEmpty(array);
  }

  /**
   * Checks if an array is empty or {@code null}.
   *
   * @param array the array to test
   * @return {@code true} if the array is empty or {@code null}
   */
  private static boolean isArrayEmpty(final Object array) {
    return getLength(array) == 0;
  }

  /**
   * Adds all the elements of the given arrays into a new array.
   *
   * <p>The new array contains all of the element of {@code array1} followed by all of the elements
   * {@code array2}. When an array is returned, it is always a new array.
   *
   * <pre>
   * ArrayUtils.addAll(null, null)     = null
   * ArrayUtils.addAll(array1, null)   = cloned copy of array1
   * ArrayUtils.addAll(null, array2)   = cloned copy of array2
   * ArrayUtils.addAll([], [])         = []
   * ArrayUtils.addAll(null, null)     = null
   * ArrayUtils.addAll([null], [null]) = [null, null]
   * ArrayUtils.addAll(["a", "b", "c"], ["1", "2", "3"]) = ["a", "b", "c", "1", "2", "3"]
   * </pre>
   *
   * @param <T> the component type of the array
   * @param array1 the first array whose elements are added to the new array, may be {@code null}
   * @param array2 the second array whose elements are added to the new array, may be {@code null}
   * @return The new array, {@code null} if both arrays are {@code null}. The type of the new array
   *     is the type of the first array, unless the first array is null, in which case the type is
   *     the same as the second array.
   * @throws IllegalArgumentException if the array types are incompatible
   * @since 2.1
   */
  public static <T> T[] addAll(final T[] array1, @SuppressWarnings("unchecked") final T... array2) {
    if (array1 == null) {
      return clone(array2);
    }
    if (array2 == null) {
      return clone(array1);
    }
    final Class<T> type1 = getComponentType(array1);
    final T[] joinedArray =
        arraycopy(
            array1, 0, 0, array1.length, () -> newInstance(type1, array1.length + array2.length));
    try {
      System.arraycopy(array2, 0, joinedArray, array1.length, array2.length);
    } catch (final ArrayStoreException ase) {
      // Check if problem was due to incompatible types
      /*
       * We do this here, rather than before the copy because: - it would be a wasted check most of the time - safer, in case check turns out to be too
       * strict
       */
      final Class<?> type2 = array2.getClass().getComponentType();
      if (!type1.isAssignableFrom(type2)) {
        throw new IllegalArgumentException(
            "Cannot store " + type2.getName() + " in an array of " + type1.getName(), ase);
      }
      throw ase; // No, so rethrow original
    }
    return joinedArray;
  }

  /**
   * Shallow clones an array or returns {@code null}.
   *
   * <p>The objects in the array are not cloned, thus there is no special handling for
   * multi-dimensional arrays.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param <T> the component type of the array
   * @param array the array to shallow clone, may be {@code null}
   * @return the cloned array, {@code null} if {@code null} input
   */
  public static <T> T[] clone(final T[] array) {
    return array != null ? array.clone() : null;
  }

  /**
   * Gets an array's component type.
   *
   * @param <T> The array type.
   * @param array The array.
   * @return The component type.
   * @since 3.13.0
   */
  public static <T> Class<T> getComponentType(final T[] array) {
    return ClassUtils.getComponentType(ObjectUtils.getClass(array));
  }

  /**
   * A fluent version of {@link System#arraycopy(Object, int, Object, int, int)} that returns the
   * destination array.
   *
   * @param <T> the type.
   * @param source the source array.
   * @param sourcePos starting position in the source array.
   * @param destPos starting position in the destination data.
   * @param length the number of array elements to be copied.
   * @param allocator allocates the array to populate and return.
   * @return dest
   * @throws IndexOutOfBoundsException if copying would cause access of data outside array bounds.
   * @throws ArrayStoreException if an element in the {@code src} array could not be stored into the
   *     {@code dest} array because of a type mismatch.
   * @throws NullPointerException if either {@code src} or {@code dest} is {@code null}.
   * @since 3.15.0
   */
  public static <T> T arraycopy(
      final T source,
      final int sourcePos,
      final int destPos,
      final int length,
      final Supplier<T> allocator) {
    return arraycopy(source, sourcePos, allocator.get(), destPos, length);
  }

  /**
   * A fluent version of {@link System#arraycopy(Object, int, Object, int, int)} that returns the
   * destination array.
   *
   * @param <T> the type
   * @param source the source array.
   * @param sourcePos starting position in the source array.
   * @param dest the destination array.
   * @param destPos starting position in the destination data.
   * @param length the number of array elements to be copied.
   * @return dest
   * @throws IndexOutOfBoundsException if copying would cause access of data outside array bounds.
   * @throws ArrayStoreException if an element in the {@code src} array could not be stored into the
   *     {@code dest} array because of a type mismatch.
   * @throws NullPointerException if either {@code src} or {@code dest} is {@code null}.
   * @since 3.15.0
   */
  public static <T> T arraycopy(
      final T source, final int sourcePos, final T dest, final int destPos, final int length) {
    System.arraycopy(source, sourcePos, dest, destPos, length);
    return dest;
  }

  /**
   * Delegates to {@link Array#newInstance(Class,int)} using generics.
   *
   * @param <T> The array type.
   * @param componentType The array class.
   * @param length the array length
   * @return The new array.
   * @throws NullPointerException if the specified {@code componentType} parameter is null.
   * @since 3.13.0
   */
  @SuppressWarnings("unchecked") // OK, because array and values are of type T
  public static <T> T[] newInstance(final Class<T> componentType, final int length) {
    return (T[]) Array.newInstance(componentType, length);
  }

  /**
   * Converts an array of object Integers to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param array a {@link Integer} array, may be {@code null}
   * @return an {@code int} array, {@code null} if null array input
   * @throws NullPointerException if an array element is {@code null}
   */
  public static int[] toPrimitive(final Integer[] array) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return EMPTY_INT_ARRAY;
    }
    final int[] result = new int[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].intValue();
    }
    return result;
  }

  /**
   * Converts an array of object Booleans to primitives.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * <p>Null array elements map to false, like {@code Boolean.parseBoolean(null)} and its callers
   * return false.
   *
   * @param array a {@link Boolean} array, may be {@code null}
   * @return a {@code boolean} array, {@code null} if null array input
   */
  public static boolean[] toPrimitive(final Boolean[] array) {
    return toPrimitive(array, false);
  }

  /**
   * Converts an array of object Booleans to primitives handling {@code null}.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param array a {@link Boolean} array, may be {@code null}
   * @param valueForNull the value to insert if {@code null} found
   * @return a {@code boolean} array, {@code null} if null array input
   */
  public static boolean[] toPrimitive(final Boolean[] array, final boolean valueForNull) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return EMPTY_BOOLEAN_ARRAY;
    }
    final boolean[] result = new boolean[array.length];
    for (int i = 0; i < array.length; i++) {
      final Boolean b = array[i];
      result[i] = b == null ? valueForNull : b.booleanValue();
    }
    return result;
  }

  /**
   * Converts an array of object Floats to primitives handling {@code null}.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param array a {@link Float} array, may be {@code null}
   * @param valueForNull the value to insert if {@code null} found
   * @return a {@code float} array, {@code null} if null array input
   */
  public static float[] toPrimitive(final Float[] array, final float valueForNull) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return EMPTY_FLOAT_ARRAY;
    }
    final float[] result = new float[array.length];
    for (int i = 0; i < array.length; i++) {
      final Float b = array[i];
      result[i] = b == null ? valueForNull : b.floatValue();
    }
    return result;
  }

  /**
   * Converts an array of object Doubles to primitives handling {@code null}.
   *
   * <p>This method returns {@code null} for a {@code null} input array.
   *
   * @param array a {@link Double} array, may be {@code null}
   * @param valueForNull the value to insert if {@code null} found
   * @return a {@code double} array, {@code null} if null array input
   */
  public static double[] toPrimitive(final Double[] array, final double valueForNull) {
    if (array == null) {
      return null;
    }
    if (array.length == 0) {
      return EMPTY_DOUBLE_ARRAY;
    }
    final double[] result = new double[array.length];
    for (int i = 0; i < array.length; i++) {
      final Double b = array[i];
      result[i] = b == null ? valueForNull : b.doubleValue();
    }
    return result;
  }
}
