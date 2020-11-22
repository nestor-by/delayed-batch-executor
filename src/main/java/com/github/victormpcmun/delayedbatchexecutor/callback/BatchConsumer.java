package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;

@FunctionalInterface
public interface BatchConsumer<A> {

  void apply(List<A> firstParam);
}