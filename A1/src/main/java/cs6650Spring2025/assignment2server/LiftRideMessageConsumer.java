package cs6650Spring2025.assignment2server;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import cs6650Spring2025.clientPart1.LiftRideEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Main class for consuming lift ride events from RabbitMQ queue and maintaining
 * a thread-safe record of skier activities.
 */
public class LiftRideMessageConsumer {
  private static final String QUEUE_NAME_PREFIX = "lift_ride_queue_";
  private static final int NUM_QUEUES = 7;
//  private static final String RABBITMQ_HOST = "localhost";
private static final String RABBITMQ_HOST = "54.70.80.157";
  private static final int RABBITMQ_PORT = 5672;
  private static final String RABBITMQ_USERNAME = "admin";
  private static final String RABBITMQ_PASSWORD = "654321";

  //TODO: adjust them
  private static final int PREFETCH_COUNT = 100; //TODO: adjust this
  private static final int NUM_CONSUMER_THREADS_PER_QUEUE = 8; //TODO: adjust the consumer thread
  private static final int TOTAL_CONSUMER_THREADS = NUM_CONSUMER_THREADS_PER_QUEUE * NUM_QUEUES;
  private static final int SLEEP_TIME = 0;
  private static final int ACK_BATCH_SIZE = 100;

  //performance metrics:
  private static final AtomicInteger processMessageCount = new AtomicInteger(0);
  private static final AtomicInteger processErrorCount = new AtomicInteger(0);
  private static final AtomicInteger totalProcessMessageCount = new AtomicInteger(0);
  private static final AtomicInteger lastReportTime = new AtomicInteger(0);
//  private static final ScheduledExecutorService metricsScheduler;

  private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private static final long startTime = System.currentTimeMillis();




//hashmap to store the skier data that consumes from the message queue
  //Key: skierID, value: liftRide details
  private static final ConcurrentHashMap<Integer, CopyOnWriteArrayList<String>> skierIdToRecords = new ConcurrentHashMap<>();

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    int availableCores = Runtime.getRuntime().availableProcessors();
    System.out.println("Available Cores: " + availableCores);
    System.out.println("Starting Skier Queue Consumer...");

    final ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(RABBITMQ_HOST);
    factory.setPort(RABBITMQ_PORT);
    factory.setUsername(RABBITMQ_USERNAME);
    factory.setPassword(RABBITMQ_PASSWORD);

    ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_CONSUMER_THREADS);
    final CountDownLatch consumerStartLatch = new CountDownLatch(TOTAL_CONSUMER_THREADS);

    try{
      final Connection connection = factory.newConnection();
      System.out.println("Connected to RabbitMQ server at " + RABBITMQ_HOST);

      // start to consume multi-thread
      for (int queIndex = 0; queIndex < NUM_QUEUES; queIndex++) {
        final String queueName = QUEUE_NAME_PREFIX + queIndex;

        //consume thread for this queue
        for (int i=0; i<NUM_CONSUMER_THREADS_PER_QUEUE; i++) {
          executorService.submit(() ->{
            try{
              consumeMessage(connection, queueName);
              consumerStartLatch.countDown();
            }catch (Exception e){
              System.err.println("Error in consumer main: " + e.getMessage());
              e.printStackTrace();
              consumerStartLatch.countDown();
            }
          });
        }
      }
    consumerStartLatch.await(10, TimeUnit.SECONDS);
      System.out.println("All consumer threads have been started successfully.");

      // Print statistics periodically
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("Shutting down RabbitMQ consumer...");
        shutdownLatch.countDown();


        try{
          if (!executorService.awaitTermination(5, TimeUnit.SECONDS));
          executorService.shutdownNow();

          if (connection.isOpen()){
            connection.close();
          }
        }catch (Exception e){
          executorService.shutdownNow();
          Thread.currentThread().interrupt();
        }
        System.out.println(
            "Consumer shutdown completed"
        );
      }));
      shutdownLatch.await();



    }catch (Exception e){
      System.err.println("Error in consumer main: " + e.getMessage());
      e.printStackTrace();
    }


  }

  private static void consumeMessage(Connection connection, String currentQueueName)  {
  try{
    final Channel channel = connection.createChannel();

    // Set prefetch count to limit the number of unacknowledged messages
    // This prevents the channel from being overwhelmed
    channel.basicQos(PREFETCH_COUNT);

    // durable: true, exclusive: false, autoDelete: false
    channel.queueDeclare(currentQueueName, true, false, false, null);


    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      try {

//        if (processMessageCount.get() < 5 || processMessageCount.get() % 1000 == 0) {
//          System.out.println("Received message: " + message);
//        }

        saveMessage(message);

        //manual acknowledgement with false auto
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true); // TODO: enable batch acknowledge
        processMessageCount.incrementAndGet();

      } catch (Exception e) {
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        processErrorCount.incrementAndGet();
        System.err.println("Error processing message: " + e.getMessage());
      }
    };


    channel.basicConsume(currentQueueName, deliverCallback, consumerTag -> {});

    while(!Thread.currentThread().isInterrupted()){
      TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);

    }

    if (channel.isOpen()){
      channel.close();

    }
  }
  catch (Exception e){
    System.err.println("Consumer thread error: " + e.getMessage());
    e.printStackTrace();
  }
  }

  /**
   * Helper method for consuming message and put it into the hashmap
   * @param message

   */

  private static void saveMessage(String message) {
    try{
      Gson gson = new Gson();
      LiftRideEvent liftRideEvent = gson.fromJson(message, LiftRideEvent.class);
      CopyOnWriteArrayList<String> skierRecords = skierIdToRecords.computeIfAbsent(
          liftRideEvent.getSkierID(), k -> new CopyOnWriteArrayList<>());
      String skierRecord = String.format("%d:%d:%s:%s:%d", liftRideEvent.getResortID(),liftRideEvent.getLiftID(), liftRideEvent.getSeasonID(), liftRideEvent.getDayID(),  liftRideEvent.getTime());
      skierRecords.add(skierRecord);
    }catch (Exception e){
      throw new RuntimeException("Failed to process message: " + e.getMessage(), e);
    }
  }

}
