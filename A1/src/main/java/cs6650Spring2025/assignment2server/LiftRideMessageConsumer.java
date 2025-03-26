package cs6650Spring2025.assignment2server;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import cs6650Spring2025.assignment3Database.SkiResortDynamoDBManager;
import cs6650Spring2025.clientPart1.LiftRideEvent;
import cs6650Spring2025.util.ConfigReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class for consuming lift ride events from RabbitMQ queue and maintaining
 * a thread-safe record of skier activities. Updated for Assignment 3
 */
public class LiftRideMessageConsumer {
  static{
    ConfigReader.loadProperties();
  }
  private static final String QUEUE_NAME_PREFIX = "lift_ride_queue_";
  private static final int NUM_QUEUES = 7;
//  private static final String RABBITMQ_HOST = "localhost";
private static final String RABBITMQ_HOST = ConfigReader.getProperty("rabbitmq.host", "54.70.80.157");

  private static final int RABBITMQ_PORT = ConfigReader.getIntProperty("rabbitmq.port", 5672);
  private static final String RABBITMQ_USERNAME = ConfigReader.getProperty("rabbitmq.username", "");
  private static final String RABBITMQ_PASSWORD = ConfigReader.getProperty("rabbitmq.password", "");


  // DynamoDB configuration
  private static final String DYNAMODB_REGION = ConfigReader.getProperty("aws.dynamodb.region", "us-east-2");
  private static final String AWS_ACCESS_KEY = ConfigReader.getProperty("aws.access.key", "");
  private static final String AWS_SECRET_KEY = ConfigReader.getProperty("aws.secret.key", "");
  //consume configg
  private static final int PREFETCH_COUNT = 50;
  private static final int NUM_CONSUMER_THREADS_PER_QUEUE = 1;
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

  private static final Logger logger = Logger.getLogger(LiftRideMessageConsumer.class.getName());
  // Circuit breaker for throttling
  private static final AtomicInteger consecutiveErrors = new AtomicInteger(0);
  private static final AtomicInteger backoffDelayMs = new AtomicInteger(0);
  private static final int ERROR_THRESHOLD = 5;
  private static final int MAX_BACKOFF_MS = 5000;
  private static final int INITIAL_BACKOFF_MS = 50;

//ADDing dynamoDB manager
  private static SkiResortDynamoDBManager dynamoDBManager;
  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    int availableCores = Runtime.getRuntime().availableProcessors();
    System.out.println("Available Cores: " + availableCores);
    System.out.println("Starting Skier Queue Consumer...");


    dynamoDBManager = new SkiResortDynamoDBManager(DYNAMODB_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY);

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

  /**
   * Consume messages from a RabbitMQ queue with optimized settings.
   * Includes circuit breaker pattern to handle back pressure.
   */
  private static void consumeMessage(Connection connection, String currentQueueName)  {
  try{
    final Channel channel = connection.createChannel();

    // Set prefetch count to limit the number of unacknowledged messages
    // This prevents the channel from being overwhelmed
    channel.basicQos(PREFETCH_COUNT);

    // durable: true, exclusive: false, autoDelete: false
    channel.queueDeclare(currentQueueName, true, false, false, null);

    // Track message count for batch acknowledgment
    final AtomicInteger batchCounter = new AtomicInteger(0);
    final AtomicLong lastDeliveryTag = new AtomicLong(0);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();
      String message = null;

      try {
        //check back pressure
        int currentBackOff = backoffDelayMs.get();

        if (currentBackOff>0){
          try {
            Thread.sleep(currentBackOff);
            // Gradually reduce backoff after successful processing
            backoffDelayMs.set(Math.max(0, currentBackOff - INITIAL_BACKOFF_MS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        //save the message to the db
        message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        saveMessage(message);



        //manual acknowledgement with false auto
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true); // TODO: enable batch acknowledge
        processMessageCount.incrementAndGet();
        consecutiveErrors.set(0);

      } catch (Exception e) {
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        processErrorCount.incrementAndGet();
        System.err.println("Error processing message: " + e.getMessage());

        try{
          channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
          if (consecutiveErrors.get() >= ERROR_THRESHOLD){
            int newBackOff = Math.min(
                backoffDelayMs.get()==0 ? INITIAL_BACKOFF_MS : backoffDelayMs.get()*2, MAX_BACKOFF_MS
            );
            backoffDelayMs.set(newBackOff);
          }
        }catch (IOException ioe) {
          logger.log(Level.SEVERE, "Failed to NACK message", ioe);
        }
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
   * Helper method for consuming message and persist to dynamoDB
   * @param message

   */

  private static void saveMessage(String message) {
    try {

      Gson gson = new Gson();
      LiftRideEvent liftRideEvent = gson.fromJson(message, LiftRideEvent.class);

      dynamoDBManager.addLiftRide(liftRideEvent);

    } catch (Exception e) {

      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}
