package com.github.victormpcmun.delayedbatchexecutor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class Tuple<A, T> {

  private final A arg;
  private final CompletableFuture<T> result;

  Tuple(A arg) {
    this.arg = arg;
    this.result = new CompletableFuture<>();
  }

  public A getArg() {
    return arg;
  }

  public CompletableFuture<T> getResult() {
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tuple<?, ?> tuple = (Tuple<?, ?>) o;
    return Objects.equals(arg, tuple.arg);
  }

  @Override
  public int hashCode() {
    return arg.hashCode();
  }
}