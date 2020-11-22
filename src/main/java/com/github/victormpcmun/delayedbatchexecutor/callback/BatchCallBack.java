package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface BatchCallBack<Z, A> {

  Map<A, Z> apply(List<A> firstParam);
}