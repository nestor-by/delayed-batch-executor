package com.github.victormpcmun.delayedbatchexecutor;

import com.github.victormpcmun.delayedbatchexecutor.callback.BatchAsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;

public class DelayedBatchExecutor<A, Z> {

  public static final int MIN_TIME_WINDOW_TIME_IN_MILLISECONDS = 1;
  public static final int MAX_TIME_WINDOW_TIME_IN_MILLISECONDS = 60 * 60 * 1000;
  public static final int DEFAULT_FIXED_THREAD_POOL_COUNTER = 4;
  public static final int DEFAULT_BUFFER_QUEUE_SIZE = 8192;
  private static final Logger log = LoggerFactory.getLogger(DelayedBatchExecutor.class);
  private final BatchAsyncCallback<Z, A> batchCallBack;
  private final Duration duration;
  private final int maxSize;
  private final ExecutorService executorService;
  private final int bufferQueueSize;
  private final Sinks.Many<Tuple<A, Z>> source;

  public DelayedBatchExecutor(Duration duration, int maxSize, ExecutorService executorService, int bufferQueueSize,
                              BatchAsyncCallback<Z, A> batchCallBack) {
    validateConfigParameters(duration, maxSize, bufferQueueSize);
    this.maxSize = maxSize;
    this.duration = duration;
    this.executorService = executorService;
    this.bufferQueueSize = bufferQueueSize;
    this.source = createProcessor(duration, maxSize, bufferQueueSize);
    this.batchCallBack = batchCallBack;
  }

  public static <Z, A> DelayedBatchExecutor<A, Z> create(
    Duration duration,
    int size,
    BatchAsyncCallback<Z, A> callBack) {
    final ExecutorService executorService = Executors.newFixedThreadPool(DEFAULT_FIXED_THREAD_POOL_COUNTER);
    return new DelayedBatchExecutor<>(duration, size, executorService, DEFAULT_BUFFER_QUEUE_SIZE, callBack);
  }

  public static <Z, A> DelayedBatchExecutor<A, Z> create(
    Duration duration,
    int size,
    ExecutorService executorService,
    int bufferQueueSize,
    BatchAsyncCallback<Z, A> batchCallback) {
    return new DelayedBatchExecutor<>(duration, size, executorService, bufferQueueSize, batchCallback);
  }

  private void validateConfigParameters(Duration duration, int maxSize, int bufferQueueSize) {
    boolean sizeValidation = maxSize >= 1;
    boolean durationValidation = duration != null
      && duration.toMillis() >= MIN_TIME_WINDOW_TIME_IN_MILLISECONDS
      && duration.toMillis() <= MAX_TIME_WINDOW_TIME_IN_MILLISECONDS;
    boolean bufferQueueSizeValidation = (bufferQueueSize >= 1);
    if (!sizeValidation || !durationValidation || !bufferQueueSizeValidation) {
      throw new RuntimeException("Illegal configuration parameters");
    }
  }

  private Sinks.Many<Tuple<A, Z>> createProcessor(Duration duration, int maxSize, int bufferQueueSize) {
    Queue<Tuple<A, Z>> blockingQueue = new ArrayBlockingQueue<>(bufferQueueSize);
    Sinks.Many<Tuple<A, Z>> newSource = Sinks.many().unicast().onBackpressureBuffer(blockingQueue);
    newSource.asFlux()
      .bufferTimeout(maxSize, duration)
      .subscribe(this::invokeBatchCallbackAndContinue);
    return newSource;
  }

  private void invokeBatchCallbackAndContinue(List<Tuple<A, Z>> tuples) {
    final List<A> args = tuples.stream().map(Tuple::getArg).collect(toList());
    batchCallBack.apply(args)
      .doOnError(e -> log.error("invokeBatchCallbackAndContinue: " + e.getMessage(), e))
      .defaultIfEmpty(Collections.emptyMap())
      .subscribeOn(Schedulers.fromExecutorService(executorService))
      .subscribe(rawResultList -> {
        for (Tuple<A, Z> tuple : tuples) {
          tuple.getResult().complete(rawResultList.get(tuple.getArg()));
        }
      }, e -> {
        for (Tuple<A, Z> tuple : tuples) {
          tuple.getResult().completeExceptionally(e);
        }
      });
  }

  public Z execute(A arg1) {
    return execute(arg1, 5, TimeUnit.SECONDS);
  }

  public Z execute(A arg1, long timeout, TimeUnit unit) {
    final CompletableFuture<Z> result = enlistTuple(arg1);
    try {
      return result.get(timeout, unit);
    } catch (ExecutionException e) {
      throw (RuntimeException) e.getCause();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted waiting. it shouldn't happen ever", e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Z> executeAsFuture(A arg1) {
    return enlistTuple(arg1);
  }

  public Mono<Z> executeAsMono(A arg1) {
    return Mono.fromFuture(() -> enlistTuple(arg1));
  }

  private CompletableFuture<Z> enlistTuple(A arg) {
    final Tuple<A, Z> param = new Tuple<>(arg);
    source.emitNext(param, (signalType, emitResult) ->
      emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED
    );
    return param.getResult();
  }

  @Override
  public String toString() {
    return "DelayedBatchExecutor "
      + "{duration=" + duration.toMillis() + ", size=" + maxSize + ", bufferQueueSize=" + bufferQueueSize + "}";
  }
}
