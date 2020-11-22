package com.github.victormpcmun.delayedbatchexecutor;

import static com.github.victormpcmun.delayedbatchexecutor.callback.BatchCallbacks.block;
import static com.github.victormpcmun.delayedbatchexecutor.callback.BatchCallbacks.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

@SuppressWarnings("squid:S2925")
public class DelayedBatchExecutorTest {

  private static final Logger log = LoggerFactory.getLogger(DelayedBatchExecutorTest.class);
  private final static String PREFIX = "P";

  private final static int CONCURRENT_THREADS = 10;

  private final static int MIN_MILLISECONDS_SIMULATION_DELAY_CALLBACK = 0;
  private final static int MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK = 2000;

  private final static Duration DBE_DURATION = Duration.ofMillis(50);
  private final static Integer DBE_MAX_SIZE = 4;

  private static void sleepCurrentThread(int milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException("InterruptedException", e);
    }
  }

  // for each integer it returns the concatenated String of constant PREFIX + integer, example: for Integer 23 it returns "P23"
  private Mono<Map<Integer, String>> delayedBatchExecutorCallbackWithSimulatedDelay(List<Integer> integerList) {
    return Mono.fromCallable(() -> {

      Map<Integer, String> stringListSimulatedResult = integerList.stream()
          .collect(Collectors.toMap(x -> x, value -> PREFIX + value));
      // simulate a delay of execution
      int millisecondsWaitSimulation = getRandomIntegerFromInterval(MIN_MILLISECONDS_SIMULATION_DELAY_CALLBACK,
          MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK);
      sleepCurrentThread(millisecondsWaitSimulation);
      log.info("BatchCallback. Simulated Exec Time {} ms.  Received {} args => {}. Returned {}. ",
          millisecondsWaitSimulation, integerList.size(), integerList, stringListSimulatedResult);

      // to force the test to fail, uncomment this:
      //stringList.set(0,"FORCE_FAILING");
      return stringListSimulatedResult;
    });
  }

  @Test
  public void blockingTest() {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallbackWithSimulatedDelay);
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 1000);
      log.info("blockingTest=>Before invoking execute with arg {}", randomInteger);
      String expectedValue =
          PREFIX + randomInteger; // the expected String returned by delayedBatchExecutorCallback for a given integer
      String result = dbe2.execute(randomInteger); // it will block until the result is available
      log.info("blockingTest=>After invoking execute. Expected returned Value {}. Actual returned value {}",
          expectedValue, result);
      Assert.assertEquals(result, expectedValue);
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void futureTest() {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallbackWithSimulatedDelay);
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 1000);
      log.info("futureTest=>Before invoking execute with arg {}", randomInteger);
      String expectedValue =
          PREFIX + randomInteger; // the expected String returned by delayedBatchExecutorCallback for a given integer
      Future<String> future = dbe2.executeAsFuture(randomInteger); // it will NOT block until the result is available
      log.info("futureTest=>Doing some computation after invoking executeAsFuture");
      String result;
      try {
        result = future.get();
        log.info("futureTest=>After invoking execute. Expected returned Value {}. Actual returned value {}",
            expectedValue, result);
        Assert.assertEquals(result, expectedValue);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void monoTest() {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, this::delayedBatchExecutorCallbackWithSimulatedDelay);
    AtomicInteger atomicIntegerCounter = new AtomicInteger(0);
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 1000);
      log.info("monoTest=>Before invoking execute with arg {}", randomInteger);
      String expectedValue =
          PREFIX + randomInteger; // the expected String returned by delayedBatchExecutorCallback for a given integer
      Mono<String> mono = dbe2.executeAsMono(randomInteger); // it will NOT block the thread
      log.info("monoTest=>Continue with computation after invoking the executeAsMono");
      mono.subscribe(result -> {
        log.info("monoTest=>Inside Mono. Expected  Value {}. Actual returned value {}", expectedValue, result);
        Assert.assertEquals(result, expectedValue);
        atomicIntegerCounter.incrementAndGet();
      });
      return null;
    };

    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
    sleepCurrentThread(
        MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK + 500); // wait time to allow all Mono's threads to finish
    Assert.assertEquals(CONCURRENT_THREADS, atomicIntegerCounter.get());
  }

  @Test(expected = NullPointerException.class)
  public void blockingExceptionTest() {
    DelayedBatchExecutor<Integer, String> dbe2LaunchingException = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, integerList -> Mono.error(new NullPointerException()));
    Callable<Void> callable = () -> {
      try {
        String result = dbe2LaunchingException.execute(1);
      } catch (NullPointerException e) {
        log.info("blockingExceptionTest=>It is capturing successfully the exception");
        throw e;  // for the purpose of this test, the exception will be rethrown in method waitUntilFinishing
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void monoExceptionTest() {
    AtomicInteger atomicIntegerCounter = new AtomicInteger(0);
    DelayedBatchExecutor<Integer, String> dbe2LaunchingException = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, integerList -> Mono.error(new NullPointerException()));
    Callable<Void> callable = () -> {
      Mono<String> mono = dbe2LaunchingException.executeAsMono(1); // it will NOT block the thread
      mono.doOnError(NullPointerException.class, e ->
      {
        log.info("monoExceptionTest=>Successfully processed the exception");
        atomicIntegerCounter.incrementAndGet();
      }).subscribe(result -> log.info("monoExceptionTest=>This should never be printed:" + result));
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
    sleepCurrentThread(
        MAX_MILLISECONDS_SIMULATION_DELAY_CALLBACK + 500); // wait time to allow all Mono's threads to finish
    Assert.assertEquals(CONCURRENT_THREADS, atomicIntegerCounter.get());
  }

  @Test(expected = NullPointerException.class)
  public void futureExceptionTest() {
    DelayedBatchExecutor<Integer, String> dbe2LaunchingException = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, integerList -> Mono.error(new NullPointerException()));
    Callable<Void> callable = () -> {
      Future<String> future = dbe2LaunchingException
          .executeAsFuture(1); // it will NOT block until the result is available
      String result = null;
      try {
        result = future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        RuntimeException actualRuntimeLaunched = (RuntimeException) e.getCause();
        if (actualRuntimeLaunched instanceof NullPointerException) {
          log.info("futureExceptionTest=>It is capturing successfully the exception");
        }
        throw actualRuntimeLaunched;
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test(expected = NullPointerException.class)
  public void changeConfigTest() {
    DelayedBatchExecutor<Integer, String> dbe2LaunchingException = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, integerList -> Mono.error(new NullPointerException()));
    Callable<Void> callable = () -> {
      Future<String> future = dbe2LaunchingException
          .executeAsFuture(1); // it will NOT block until the result is available
      try {
        future.get(); // will launch exception
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        RuntimeException actualRuntimeLaunched = (RuntimeException) e.getCause();
        if (actualRuntimeLaunched instanceof NullPointerException) {
          log.info("futureExceptionTest=>It is capturing successfully the exception");
        }
        throw actualRuntimeLaunched;
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test(expected = TimeoutException.class)
  public void futureTimeOutTest() throws InterruptedException, ExecutionException, TimeoutException {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(Duration.ofMillis(2000), 2, resultList -> Mono.just(Collections.emptyMap()));
    Future<String> futureResult1 = dbe2.executeAsFuture(1);
    futureResult1.get(1500, TimeUnit.MILLISECONDS);
  }

  //@Test
  public void extremeLargeSizeTest() {
    int fixedThreadPoolSize = 10;
    int bufferQueueSize = 40000;
    Duration duration = Duration.ofSeconds(2);
    int maxSize = 30000;
    final ExecutorService executorService = Executors.newFixedThreadPool(fixedThreadPoolSize);
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(duration, maxSize, executorService, bufferQueueSize, block(integerList -> {
          Map<Integer, String> stringListSimulatedResult = integerList.stream()
              .collect(Collectors.toMap(x -> x, value -> PREFIX + value));
          log.info("BatchCallback.  Received {} args => {}. Returned {}. ", integerList.size(), integerList,
              stringListSimulatedResult);
          return stringListSimulatedResult;
        }));
    Callable<Void> callable = () -> {
      for (int i = 0; i < 20; i++) {

        Integer randomInteger = getRandomIntegerFromInterval(1, 1000);
        String expectedValue =
            PREFIX + randomInteger; // the expected String returned by delayedBatchExecutorCallback for a given integer
        String result = dbe2.execute(randomInteger); // it will block until the result is available
        Assert.assertEquals(result, expectedValue);
        //dbe2.updateConfig(dbe2.getDuration(), dbe2.getMaxSize(), dbe2.getExecutorService(), dbe2.getBufferQueueSize()+1);
        log.info("DelayedBatchExecutor:{}", dbe2);
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(30000, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void nullInResponseTest() {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, listOfIntegers -> {
          return Mono.empty();
        });
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 1000);
      String result = dbe2.execute(randomInteger); // it will block until the result is available
      Assert.assertNull(result);
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void nullInSomePositionsInResultTest() {
    DelayedBatchExecutor<Integer, String> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, block(integerList -> integerList.stream()
            .collect(HashMap::new, (m, v) -> m.put(v, v % 2 == 0 ? null : PREFIX + v), HashMap::putAll)));
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 3);
      String result = dbe2.execute(randomInteger); // it will block until the result is available
      if (randomInteger == 2) {
        Assert.assertNull(result);
      } else {
        Assert.assertEquals(result, PREFIX + randomInteger);
      }
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    waitUntilFinishing(threadsAsFutures);
  }

  @Test
  public void voidTest() {
    DelayedBatchExecutor<Integer, Void> dbe2 = DelayedBatchExecutor
        .create(DBE_DURATION, DBE_MAX_SIZE, consumer(listOfIntegers -> {
          log.info("received: {}", listOfIntegers);
        }));
    Callable<Void> callable = () -> {
      Integer randomInteger = getRandomIntegerFromInterval(1, 100);
      Future<Void> result = dbe2.executeAsFuture(randomInteger);

      sleepCurrentThread(500);
      return null;
    };
    List<Future<Void>> threadsAsFutures = createAndStartThreadsForCallable(CONCURRENT_THREADS, callable);
    Assert.assertNotNull(threadsAsFutures);
    waitUntilFinishing(threadsAsFutures);
  }

  private void waitUntilFinishing(List<Future<Void>> threads) {
    for (Future future : threads) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw (RuntimeException) e.getCause();
      }
    }
  }

  private List<Future<Void>> createAndStartThreadsForCallable(int threadsCount, Callable callable) {
    ExecutorService es = Executors.newFixedThreadPool(threadsCount);
    List<Future<Void>> threads = new ArrayList<>();
    for (int threadCounter = 0; threadCounter < threadsCount; threadCounter++) {
      threads.add(es.submit(callable));
    }
    return threads;
  }

  private Integer getRandomIntegerFromInterval(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max + 1);
  }
}