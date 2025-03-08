package cs6650Spring2025.clientPart1;

import cs6650Spring2025.clientPart2.RecordWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SkierClientMain {

  private static final int TOTAL_REQUESTS = 200000;
  private static final int NUM_THREADS = 32;

  private static final int POST_REQUEST_PER_THREAD = 1000;
  private static final int MILLISECONDS = 1000;
  private static final double PHASE_1_PERCENTAGE = 0.2;

  public static void main(String[] args) throws InterruptedException {
    //start count the time and set up the counts for failure and success
    long startTime = System.currentTimeMillis();
    AtomicInteger successCounts = new AtomicInteger(0);
    AtomicInteger failCounts = new AtomicInteger(0);
    RecordWriter recordWriter = new RecordWriter("PostRequestMetrics.csv");

    // start to generate the random lift ride event
    LiftRideEventRandomGenerator liftRideEventRandomGenerator = new LiftRideEventRandomGenerator();
    liftRideEventRandomGenerator.startGeneration();

    //For the first 1000 post request
    ExecutorService executorService1 = Executors.newFixedThreadPool(NUM_THREADS);
    CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);
    int phase1ThreadsToWaitFor = (int) Math.max(1, NUM_THREADS*PHASE_1_PERCENTAGE);
    CountDownLatch countDownLatchPartial = new CountDownLatch(phase1ThreadsToWaitFor);

    System.out.println("Starting Phase 1 with " + NUM_THREADS + " threads");
    System.out.println("phase 1 total request:" + (NUM_THREADS * POST_REQUEST_PER_THREAD));
    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      executorService1.submit(
        new PostRequestSingleThread(POST_REQUEST_PER_THREAD, successCounts, failCounts, countDownLatch, liftRideEventRandomGenerator, recordWriter)
      );
      if (threadNum<phase1ThreadsToWaitFor){
        countDownLatchPartial.countDown();
      }
    }

    countDownLatchPartial.await();
    System.out.println("Starting Phase 2 after " + phase1ThreadsToWaitFor + " threads from Phase 1 completed");


    // for the remaining posts, create 2* core
    int remainingRequests = TOTAL_REQUESTS - (NUM_THREADS * POST_REQUEST_PER_THREAD);
    System.out.println("Remaining requests for Phase 2: " + remainingRequests);
    if (remainingRequests > 0) {
      int availableCores = Runtime.getRuntime().availableProcessors();
      int remainingThread = availableCores * 2;
      int remainingRequestPerThread = remainingRequests / remainingThread;
      int extraRemainingRequestPerThread = remainingRequests % remainingThread;

      ExecutorService executorService2 = Executors.newFixedThreadPool(remainingThread);
      CountDownLatch countDownLatch2 = new CountDownLatch(remainingThread);

      System.out.println("Starting Phase 2 with " + remainingThread + " threads");
      for (int i = 0; i < remainingThread; i++) {
        int threadRequest = remainingRequestPerThread + (i<extraRemainingRequestPerThread ? 1 : 0);
        executorService2.submit(new PostRequestSingleThread(threadRequest, successCounts, failCounts, countDownLatch2, liftRideEventRandomGenerator, recordWriter));
      }
      countDownLatch2.await();
      executorService2.shutdown();
    }

    countDownLatch.await();
    executorService1.shutdown();

    // stop the generation
    liftRideEventRandomGenerator.stopGeneration();
    long endTime = System.currentTimeMillis();
    long wallTime = endTime - startTime;
    double throughput =(double) TOTAL_REQUESTS / (wallTime/MILLISECONDS);

    //printout:
    //number of successful requests sent
    //number of unsuccessful requests (should be 0)
    //the total run time (wall time) for all phases to complete. Calculate this by taking a timestamp before you startany threads and another after all threads are complete.
    //the total throughput in requests per second (total number of requests/wall time)
    System.out.println("Successful requests: " + successCounts.get());
    System.out.println("Failed requests: " + failCounts.get());
    System.out.println("Wall Time: " + wallTime + " milliseconds");
    System.out.println("Throughput: " + String.format("%.2f", throughput) + " requests/second");

    recordWriter.calculateStatistics();

  }
}
