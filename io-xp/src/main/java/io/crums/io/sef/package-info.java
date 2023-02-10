/*
 * Copyright 2022 Babak Farhang
 */
/**
 * Simplified Elias-Fano encoding for a compressed, random access bag of
 * ascending integers (maximum 64-, err no unsigneds in Java, 63-bits).
 * <p>
 * This design is inspired by this
 * <a href="https://www.reddit.com/r/algorithms/comments/ylcu0j/space_efficient_random_access_list_of_ascending/">
 * Reddit discussion</a>.
 * </p>
 * @see io.crums.io.sef.Alf
 */
package io.crums.io.sef;
