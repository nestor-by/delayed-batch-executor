package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface BatchAsyncCallback<Z, A> {

  Mono<Map<A, Z>> apply(List<A> firstParam);
}