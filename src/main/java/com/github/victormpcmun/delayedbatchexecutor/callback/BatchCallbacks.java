package com.github.victormpcmun.delayedbatchexecutor.callback;

import java.util.Collections;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;

public class BatchCallbacks {

  private BatchCallbacks() {
  }

  public static <A, Z> BatchAsyncCallback<A, Z> block(BatchCallBack<A, Z> callBack) {
    return params -> Mono.fromCallable(() -> callBack.apply(params));
  }

  public static <A, Z> BatchAsyncCallback<A, Z> distinct(BatchAsyncCallback<A, Z> callBack) {
    return params -> callBack.apply(params.stream().distinct().collect(Collectors.toList()));
  }

  public static <A, Z> BatchAsyncCallback<A, Z> consumer(BatchConsumer<Z> callBack) {
    return params -> Mono.fromCallable(() -> {
      callBack.apply(params);
      return Collections.emptyMap();
    });
  }
}