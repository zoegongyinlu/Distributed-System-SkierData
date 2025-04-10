package cs6650Spring2025.assignment3Database;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import cs6650Spring2025.clientPart1.LiftRideEvent;

import cs6650Spring2025.util.ConfigReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Optimized manager class for DynamoDB operations for ski resort data.
 */
public class SkiResortDynamoDBManager {
  static {
    ConfigReader.loadProperties();
  }
  private static final Logger logger = Logger.getLogger(SkiResortDynamoDBManager.class.getName());

  private static final String TABLE_NAME = ConfigReader.getProperty("dynamodb.table.name", "SkierLiftRides");
  private static final String GSI1_NAME = ConfigReader.getProperty("dynamodb.gsi1.name","ResortDaySkiers");
  private static final String GSI2_NAME = ConfigReader.getProperty("dynamodb.gsi2.name","SkierDaysInSeason");
  private static final String AWS_SESSION_TOKEN = ConfigReader.getProperty("aws.sessionToken", "");
  private final String region;
  private final String accessKey;
  private final String secretKey;

  // DynamoDB clients
  private final AmazonDynamoDB client;
  private final DynamoDB dynamoDB;
  private Table skierTable;

  // Batch write configuration
  private static final int BATCH_SIZE = 25; // Maximum allowed by DynamoDB
  private final ConcurrentLinkedQueue<Item> itemQueue = new ConcurrentLinkedQueue<>();

  private final int PROCESSING_BATCH_TIME_INTERVAL = 200;
  private final ScheduledExecutorService batchWriteService;
  private final ExecutorService batchProcessService;
//  private final ExecutorService autoScalingService;

  // Add these fields to your class
  private final TokenBucket writeLimiter;
  private static final int TOKENS_PER_SECOND = 800; // Set to ~80% of your provisioned capacity
  private static final int MAX_BURST = 200;

  // Metrics
  private final AtomicInteger queueSize = new AtomicInteger(0);
  private final AtomicInteger totalWrites = new AtomicInteger(0);
  private final AtomicInteger failedWrites = new AtomicInteger(0);
  private final AtomicInteger successfulBatches = new AtomicInteger(0);
  private final AtomicInteger failedBatches = new AtomicInteger(0);

  // Circuit breaker for throttling
  private final AtomicInteger consecutiveErrors = new AtomicInteger(0);
  private final AtomicInteger backoffDelayMs = new AtomicInteger(0);
  private static final int ERROR_THRESHOLD = 5;
  private static final int MAX_BACKOFF_MS = 5000;
  private static final int INITIAL_BACKOFF_MS = 50;

  public SkiResortDynamoDBManager(String region, String accessKey, String secretKey) {
    this.region = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;



    // Initialize clients with optimized configuration
    BasicSessionCredentials sessionCreds = new BasicSessionCredentials(
        accessKey,
        secretKey,
        AWS_SESSION_TOKEN);

    this.client = AmazonDynamoDBClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(sessionCreds))
        .withClientConfiguration(new ClientConfiguration(new com.amazonaws.ClientConfiguration()).withMaxConnections(25).withConnectionTimeout(5000).withRequestTimeout(30000).withMaxErrorRetry(10).withThrottledRetries(true))

        .build();
    this.dynamoDB = new DynamoDB(client);

    // Create table if it doesn't exist
    createTable();
    updateGSIThroughput();

    this.skierTable = this.dynamoDB.getTable(TABLE_NAME);

    // Initialize batch processing with more threads
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    this.batchWriteService = Executors.newScheduledThreadPool(availableProcessors * 2);
    this.batchProcessService = Executors.newFixedThreadPool(availableProcessors * 2);
//    this.autoScalingService = Executors.newSingleThreadExecutor();

    this.writeLimiter = new TokenBucket(TOKENS_PER_SECOND, MAX_BURST);
    // Schedule batch processing at shorter intervals
    this.batchWriteService.scheduleAtFixedRate(
        this::processBatch, 0, PROCESSING_BATCH_TIME_INTERVAL, TimeUnit.MILLISECONDS);

    // Report stats less frequently to reduce overhead
    this.batchWriteService.scheduleAtFixedRate(
        this::reportStats, 0, 5000, TimeUnit.MILLISECONDS);
    this.batchWriteService.scheduleAtFixedRate(
        () -> writeLimiter.refill(), 100, 100, TimeUnit.MILLISECONDS);
  }

  /**
   * Helper function for creating the DynamoDB table if it doesn't exist with the new schema.
   */
  private synchronized void createTable() {
    try {
      dynamoDB.getTable(TABLE_NAME).describe();
      logger.info("Table:" + TABLE_NAME + " already created");
      return;
    } catch (Exception e) {
      logger.info("error creating table:" + TABLE_NAME + " error:" + e.getMessage());
      logger.info("Table " + TABLE_NAME + " does not exist. Creating...");

      // Define attribute definitions
      List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
      attributeDefinitions.add(new AttributeDefinition("skierId", ScalarAttributeType.N));
      attributeDefinitions.add(new AttributeDefinition("seasonId_dayId_liftId_time", ScalarAttributeType.S));
      attributeDefinitions.add(new AttributeDefinition("resortId", ScalarAttributeType.N));
      attributeDefinitions.add(new AttributeDefinition("seasonId_dayId_skierId", ScalarAttributeType.S));
      attributeDefinitions.add(new AttributeDefinition("seasonId_dayId", ScalarAttributeType.S));

      // Define key schema (primary key)
      List<KeySchemaElement> keySchema = new ArrayList<>();
      keySchema.add(new KeySchemaElement("skierId", KeyType.HASH));
      keySchema.add(new KeySchemaElement("seasonId_dayId_liftId_time", KeyType.RANGE));

      // Define GSI1 - Resort Day Skiers
      GlobalSecondaryIndex gsi_resortDaySkiers = new GlobalSecondaryIndex()
          .withIndexName(GSI1_NAME)
          .withProvisionedThroughput(new ProvisionedThroughput(500L, 500L))
          .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

      List<KeySchemaElement> gsi1KeySchema = new ArrayList<>();
      gsi1KeySchema.add(new KeySchemaElement("resortId", KeyType.HASH));
      gsi1KeySchema.add(new KeySchemaElement("seasonId_dayId_skierId", KeyType.RANGE));
      gsi_resortDaySkiers.setKeySchema(gsi1KeySchema);

      // Define GSI2 - SkierDaysInSeason
      GlobalSecondaryIndex gsi2_SkierDaysInSeason = new GlobalSecondaryIndex()
          .withIndexName(GSI2_NAME)
          .withProvisionedThroughput(new ProvisionedThroughput(500L, 500L))
          .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

      List<KeySchemaElement> gsi2KeySchema = new ArrayList<>();
      gsi2KeySchema.add(new KeySchemaElement("skierId", KeyType.HASH));
      gsi2KeySchema.add(new KeySchemaElement("seasonId_dayId", KeyType.RANGE));
      gsi2_SkierDaysInSeason.setKeySchema(gsi2KeySchema);

      // Create Table with higher throughput
      CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableName(TABLE_NAME)
          .withKeySchema(keySchema)
          .withAttributeDefinitions(attributeDefinitions)
          .withGlobalSecondaryIndexes(gsi_resortDaySkiers, gsi2_SkierDaysInSeason)
          .withProvisionedThroughput(new ProvisionedThroughput(500L, 500L));

      Table table = dynamoDB.createTable(createTableRequest);
      try {
        table.waitForActive();
        logger.info("Table:" + TABLE_NAME + " successfully created");

        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
            .withTableName(TABLE_NAME)
            .withGlobalSecondaryIndexUpdates(new GlobalSecondaryIndexUpdate().withUpdate(new UpdateGlobalSecondaryIndexAction().
                withIndexName(GSI1_NAME).withProvisionedThroughput(new ProvisionedThroughput(100L, 500L))),
                new GlobalSecondaryIndexUpdate().withUpdate(new UpdateGlobalSecondaryIndexAction().withIndexName(GSI2_NAME).
                    withProvisionedThroughput(new ProvisionedThroughput(100L, 500L))));

        client.updateTable(updateTableRequest);
      } catch (InterruptedException ie) {
        logger.log(Level.SEVERE, "Table creation interrupted", ie);
        Thread.currentThread().interrupt();
      }
    }
  }


  /**
    update GSI throughput for avoiding throttling
   */
  public void updateGSIThroughput() {
    try {
      logger.info("Updating GSI throughput to handle higher load");

      // Increase GSI throughput to match the main table
      UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableName(TABLE_NAME)
          .withGlobalSecondaryIndexUpdates(
              new GlobalSecondaryIndexUpdate()
                  .withUpdate(new UpdateGlobalSecondaryIndexAction()
                      .withIndexName(GSI1_NAME)
                      .withProvisionedThroughput(new ProvisionedThroughput(500L, 500L))),
              new GlobalSecondaryIndexUpdate()
                  .withUpdate(new UpdateGlobalSecondaryIndexAction()
                      .withIndexName(GSI2_NAME)
                      .withProvisionedThroughput(new ProvisionedThroughput(500L, 500L)))
          );

      client.updateTable(updateTableRequest);
      logger.info("GSI throughput update request submitted");

    } catch (Exception e) {
      logger.log(Level.WARNING, "Error updating GSI throughput", e);
    }
  }

  /**
   * Add a lift ride from LiftRideEvent to be persisted in DynamoDB.
   * Includes throttling with circuit breaker pattern.
   */
  public void addLiftRide(LiftRideEvent liftRideEvent) {
    // Check if we're in backoff mode due to throttling
    int currentBackoff = backoffDelayMs.get();
    //circuit breaker pattern implicit half-open behavior with backoff graduated recovery
    if (currentBackoff > 0) {
      try {
        Thread.sleep(currentBackoff);
        //Each operation reduces the backoff delay by INITIAL_BACKOFF_MS (50ms)
        backoffDelayMs.set(Math.max(0, currentBackoff - INITIAL_BACKOFF_MS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      int skierId = liftRideEvent.getSkierID();
      int resortId = liftRideEvent.getResortID();
      String seasonId = liftRideEvent.getSeasonID();
      String dayId = liftRideEvent.getDayID();
      int liftId = liftRideEvent.getLiftID();
      int time = liftRideEvent.getTime();

      int vertical = liftId * 10;

      // Primary key structure
      String primarySortKey = String.format("%s_%s_%d_%d", seasonId, dayId, liftId, time);

      // GSI structure
      String gsi1SortKey = String.format("%s_%s_%d", seasonId, dayId, skierId);
      String gsi2SortKey = String.format("%s_%s", seasonId, dayId);

      // Create item
      Item item = new Item()
          .withPrimaryKey("skierId", skierId, "seasonId_dayId_liftId_time", primarySortKey)
          .withNumber("resortId", resortId)
          .withString("seasonId", seasonId)
          .withString("dayId", dayId)
          .withNumber("liftId", liftId)
          .withNumber("time", time)
          .withString("seasonId_dayId_skierId", gsi1SortKey)
          .withString("seasonId_dayId", gsi2SortKey)
          .withNumber("vertical", vertical);

      // Add to queue
      itemQueue.add(item);
      queueSize.incrementAndGet();

      // Reset error counter on successful add
      consecutiveErrors.set(0);

      // Process batch if queue size reaches threshold
      if (queueSize.get() >= BATCH_SIZE) {
        processBatch();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error adding lift ride to queue", e);
      failedWrites.incrementAndGet();

      // Increment error counter and apply backoff if needed
      // circuit open state:
      if (consecutiveErrors.incrementAndGet() >= ERROR_THRESHOLD) {
        int newBackoff = Math.min(
            backoffDelayMs.get() == 0 ? INITIAL_BACKOFF_MS : backoffDelayMs.get() * 2,
            MAX_BACKOFF_MS);
        backoffDelayMs.set(newBackoff);
        logger.warning("Throttling activated. Backoff set to " + newBackoff + "ms");
      }
    }
  }

  /**
   * Process a batch of records from the queue to the table
   * with improved error handling and retry logic.
   */
  private void processBatch() {
    if (queueSize.get() == 0) return;

    batchProcessService.submit(() -> {
      List<Item> batch = new ArrayList<>(BATCH_SIZE);
      int count = 0;

      int effectiveBatchSize = determineOptimalBatchSize();

      // Get records up to batch size
      while (count < BATCH_SIZE && !itemQueue.isEmpty()) {
        Item item = itemQueue.poll();
        if (item != null) {
          batch.add(item);
          count++;
          queueSize.decrementAndGet();
        }
      }

      if (batch.isEmpty()) {
        return;
      }

      try {
        try {
          // Wait for enough tokens before proceeding

          if (!writeLimiter.tryConsume(batch.size())) {
            // If we can't get tokens immediately, add items back to queue and return
            for (Item item : batch) {
              itemQueue.add(item);
              queueSize.incrementAndGet();
            }
            return; // Will try again on the next scheduler tick
          }
        } catch (Exception e) {
          logger.warning("Rate limiter error: " + e.getMessage());
          // Fall through and try the operation anyway
        }
        TableWriteItems tableWriteItems = new TableWriteItems(TABLE_NAME);
        tableWriteItems.withItemsToPut(batch);

        Map<String, List<WriteRequest>> unprocessedItems = executeBatchWriteWithRetry(tableWriteItems);

        // Count successful writes
        int successfulWrites = batch.size() - getTotalUnprocessedItems(unprocessedItems);
        totalWrites.addAndGet(successfulWrites);

        if (successfulWrites == batch.size()) {
          successfulBatches.incrementAndGet();
        }

        // Handle any unprocessed items
        if (!unprocessedItems.isEmpty() && unprocessedItems.containsKey(TABLE_NAME)) {
          List<WriteRequest> failedRequests = unprocessedItems.get(TABLE_NAME);
          int failedCount = failedRequests.size();

          failedWrites.addAndGet(failedCount);
          failedBatches.incrementAndGet();
          logger.warning("Failed to process " + failedCount + " items. Adding back to queue.");

          // Apply throttling if we have failures
          if (failedCount > 0) {
            if (consecutiveErrors.incrementAndGet() >= ERROR_THRESHOLD) {
              int newBackoff = Math.min(
                  backoffDelayMs.get() == 0 ? INITIAL_BACKOFF_MS : backoffDelayMs.get() * 2,
                  MAX_BACKOFF_MS);
              backoffDelayMs.set(newBackoff);
              logger.warning("Throttling activated during batch processing. Backoff set to " + newBackoff + "ms");
            }
          } else {
            consecutiveErrors.set(0);
          }

          // Re-add failed items to the queue
          reAddFailedItems(failedRequests);
        } else {
          // Reset error counter on successful batch
          consecutiveErrors.set(0);
          if (backoffDelayMs.get() > 0) {
            logger.info("Throttling deactivated");
            backoffDelayMs.set(0);
          }
        }
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Error in batch write", e);
        failedWrites.addAndGet(batch.size());
        failedBatches.incrementAndGet();

        // Re-add items to queue
        for (Item item : batch) {
          itemQueue.add(item);
          queueSize.incrementAndGet();
        }

        // Apply throttling on exception
        if (consecutiveErrors.incrementAndGet() >= ERROR_THRESHOLD) {
          int newBackoff = Math.min(
              backoffDelayMs.get() == 0 ? INITIAL_BACKOFF_MS : backoffDelayMs.get() * 2,
              MAX_BACKOFF_MS);
          backoffDelayMs.set(newBackoff);
          logger.warning("Throttling activated due to exception. Backoff set to " + newBackoff + "ms");
        }
      }
    });
  }
  /**
   * Determine optimal batch size based on current conditions
   */
  private int determineOptimalBatchSize(){
    int currentQueSize = queueSize.get();
    int errorCount = consecutiveErrors.get();

    int batchSize = BATCH_SIZE;
    if (errorCount>0){
      batchSize = Math.max(1, BATCH_SIZE/(errorCount+1));
    }

    if (currentQueSize>50000){
      batchSize = (errorCount==0)? BATCH_SIZE : batchSize;
    }else if (currentQueSize < 1000 && errorCount > 0){
      batchSize = Math.max(1, batchSize / 2);
    }

    return batchSize;
  }

  /**
   * Re-add failed items to the queue with better attribute extraction.
   */
  private void reAddFailedItems(List<WriteRequest> failedRequests) {
    for (WriteRequest request : failedRequests) {
      if (request.getPutRequest() != null) {
        Map<String, AttributeValue> attributes = request.getPutRequest().getItem();

        // Extract primary key components
        int skierId = Integer.parseInt(attributes.get("skierId").getN());
        String sortKey = attributes.get("seasonId_dayId_liftId_time").getS();

        // Extract other attributes with null checks
        int resortId = getIntAttribute(attributes, "resortId");
        String seasonId = getStringAttribute(attributes, "seasonId");
        String dayId = getStringAttribute(attributes, "dayId");
        int liftId = getIntAttribute(attributes, "liftId");
        int time = getIntAttribute(attributes, "time");
        int vertical = getIntAttribute(attributes, "vertical");
        String gsi1SortKey = getStringAttribute(attributes, "seasonId_dayId_skierId");
        String gsi2SortKey = getStringAttribute(attributes, "seasonId_dayId");

        // Recreate the item
        Item failedItem = new Item()
            .withPrimaryKey("skierId", skierId, "seasonId_dayId_liftId_time", sortKey)
            .withNumber("resortId", resortId)
            .withString("seasonId", seasonId)
            .withString("dayId", dayId)
            .withNumber("liftId", liftId)
            .withNumber("time", time)
            .withString("seasonId_dayId_skierId", gsi1SortKey)
            .withString("seasonId_dayId", gsi2SortKey)
            .withNumber("vertical", vertical);

        itemQueue.add(failedItem);
        queueSize.incrementAndGet();
      }
    }
  }

  // Helper methods for attribute extraction with null checks
  private int getIntAttribute(Map<String, AttributeValue> attributes, String key) {
    AttributeValue value = attributes.get(key);
    return (value != null && value.getN() != null) ? Integer.parseInt(value.getN()) : 0;
  }

  private String getStringAttribute(Map<String, AttributeValue> attributes, String key) {
    AttributeValue value = attributes.get(key);
    return (value != null && value.getS() != null) ? value.getS() : "";
  }

  /**
   * Count the total number of unprocessed items.
   */
  private int getTotalUnprocessedItems(Map<String, List<WriteRequest>> unprocessedItems) {
    if (unprocessedItems == null || unprocessedItems.isEmpty()) {
      return 0;
    }

    int count = 0;
    for (List<WriteRequest> requests : unprocessedItems.values()) {
      count += requests.size();
    }
    return count;
  }

  /**
   * Report stats with more detailed metrics.
   */
  private void reportStats() {
    int currentQueueSize = queueSize.get();
    int currentTotalWrites = totalWrites.get();
    int currentFailedWrites = failedWrites.get();
    int currentBackoff = backoffDelayMs.get();
    int currentSuccessBatches = successfulBatches.get();
    int currentFailedBatches = failedBatches.get();

    logger.info(String.format(
        "DynamoDB Stats - Queue Size: %d, Total Writes: %d, Failed Writes: %d, " +
            "Successful Batches: %d, Failed Batches: %d, Current Backoff: %dms",
        currentQueueSize, currentTotalWrites, currentFailedWrites,
        currentSuccessBatches, currentFailedBatches, currentBackoff));
  }

  /**
   * Execute a batch write with optimized exponential backoff retry strategy.
   */
  private Map<String, List<WriteRequest>> executeBatchWriteWithRetry(TableWriteItems tableWriteItems) {
    int maxRetries = 20; // Increased from 5
    int baseDelay = 100; // ms
    double jitterPct = 0.3;
    Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        // Execute batch write
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
        unprocessedItems = outcome.getUnprocessedItems();

        // If no unprocessed items, we're done
        if (unprocessedItems.isEmpty()) {
          return unprocessedItems;
        }

        // Check for capacity-related errors
        if (attempt < maxRetries - 1) {
          // Add exponential backoff with jitter
          int delay = baseDelay * (1 << attempt);
          int jitter = (int) (delay * jitterPct * Math.random());
          Thread.sleep(delay + jitter);
          logger.info("Throttling detected, backing off for " + (delay + jitter) + "ms before retry " + (attempt + 1));
        }

        // For the next attempt, only retry the unprocessed items
        tableWriteItems = createTableWriteItemsFromUnprocessed(unprocessedItems);

        // If there are no items to retry, break out of the loop
        if (tableWriteItems.getItemsToPut() == null || tableWriteItems.getItemsToPut().isEmpty()) {
          break;
        }

      } catch (Exception e) {
        logger.log(Level.WARNING, "Error in batch write attempt " + attempt, e);

        // Back off before retry
        try {
          int delay = baseDelay * (1 << attempt);
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    return unprocessedItems;
  }

  /**
   * Create TableWriteItems from unprocessed items with better error handling.
   */
  private TableWriteItems createTableWriteItemsFromUnprocessed(Map<String, List<WriteRequest>> unprocessedItems) {
    TableWriteItems tableWriteItems = new TableWriteItems(TABLE_NAME);
    List<WriteRequest> failedRequests = unprocessedItems.getOrDefault(TABLE_NAME, Collections.emptyList());
    List<Item> itemsToRetry = new ArrayList<>();

    for (WriteRequest request : failedRequests) {
      if (request.getPutRequest() != null) {
        Map<String, AttributeValue> attributes = request.getPutRequest().getItem();

        // Helper functions for safe attribute extraction
        Function<String, String> getStringAttr = key -> {
          AttributeValue attr = attributes.get(key);
          return (attr != null && attr.getS() != null) ? attr.getS() : "";
        };

        Function<String, Integer> getIntAttr = key -> {
          AttributeValue attr = attributes.get(key);
          return (attr != null && attr.getN() != null) ? Integer.parseInt(attr.getN()) : 0;
        };

        // Extract primary key
        int skierId = getIntAttr.apply("skierId");
        String sortKey = getStringAttr.apply("seasonId_dayId_liftId_time");

        // Only proceed if we have the primary key
        if (skierId > 0 && !sortKey.isEmpty()) {
          Item item = new Item()
              .withPrimaryKey("skierId", skierId, "seasonId_dayId_liftId_time", sortKey);

          // Add all other attributes
          attributes.forEach((key, value) -> {
            if (!key.equals("skierId") && !key.equals("seasonId_dayId_liftId_time")) {
              if (value.getN() != null) {
                item.withNumber(key, Double.parseDouble(value.getN()));
              } else if (value.getS() != null) {
                item.withString(key, value.getS());
              }
            }
          });

          itemsToRetry.add(item);
        }
      }
    }

    if (!itemsToRetry.isEmpty()) {
      tableWriteItems.withItemsToPut(itemsToRetry);
    }

    return tableWriteItems;
  }

  /**
   * Get count of days a skier has skied in a season.
   */
  public int getSkierDayInSeason(int skierId, String seasonId) {
    try {
      QuerySpec querySpec = new QuerySpec()
          .withKeyConditionExpression("skierId = :skierId and begins_with(seasonId_dayId, :seasonPrefix)")
          .withValueMap(new ValueMap()
              .withNumber(":skierId", skierId)
              .withString(":seasonPrefix", seasonId + "_"));

      ItemCollection<QueryOutcome> items = skierTable.getIndex(GSI2_NAME).query(querySpec);

      // Collect distinct days
      Set<String> uniqueDays = new HashSet<>();
      for (Item item : items) {
        String seasonDayKey = item.getString("seasonId_dayId");
        uniqueDays.add(seasonDayKey);
      }

      return uniqueDays.size();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error getting skier days in season", e);
      return 0;
    }
  }

  /**
   * Get vertical totals for a skier by day.
   */
  public Map<String, Integer> getSkierVerticalByDay(int skierId) {
    try {
      QuerySpec querySpec = new QuerySpec()
          .withKeyConditionExpression("skierId = :skierId")
          .withValueMap(new ValueMap().withNumber(":skierId", skierId));

      ItemCollection<QueryOutcome> items = skierTable.query(querySpec);
      Map<String, Integer> dayVerticals = new HashMap<>();

      for (Item item : items) {
        String seasonId = item.getString("seasonId");
        String dayId = item.getString("dayId");
        String dayKey = seasonId + "_" + dayId;
        int vertical = item.getInt("vertical");

        // Sum up verticals for the same day
        dayVerticals.merge(dayKey, vertical, Integer::sum);
      }

      return dayVerticals;
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error getting skier vertical by day", e);
      return Collections.emptyMap();
    }
  }

  /**
   * Get lifts ridden by a skier on each day.
   */
  public Map<String, Set<Integer>> getSkierLiftsByDay(int skierId) {
    try {
      QuerySpec querySpec = new QuerySpec()
          .withKeyConditionExpression("skierId = :skierId")
          .withValueMap(new ValueMap().withNumber(":skierId", skierId));

      ItemCollection<QueryOutcome> items = skierTable.query(querySpec);
      Map<String, Set<Integer>> dayLifts = new HashMap<>();

      for (Item item : items) {
        String seasonId = item.getString("seasonId");
        String dayId = item.getString("dayId");
        String dayKey = seasonId + "_" + dayId;
        int liftId = item.getInt("liftId");

        dayLifts.computeIfAbsent(dayKey, k -> new HashSet<>()).add(liftId);
      }

      return dayLifts;
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error getting skier lifts by day", e);
      return Collections.emptyMap();
    }
  }

  /**
   * Get count of unique skiers at a resort on a specific day.
   */
  public int getUniqueSkiersCount(int resortId, String seasonId, String dayId) {
    try {
      String seasonDayPrefix = seasonId + "_" + dayId + "_";

      QuerySpec querySpec = new QuerySpec()
          .withKeyConditionExpression("resortId = :resortId and begins_with(seasonId_dayId_skierId, :prefix)")
          .withValueMap(new ValueMap()
              .withNumber(":resortId", resortId)
              .withString(":prefix", seasonDayPrefix));

      ItemCollection<QueryOutcome> items = skierTable.getIndex(GSI1_NAME).query(querySpec);

      // Count distinct skiers
      Set<Integer> uniqueSkiers = new HashSet<>();
      for (Item item : items) {
        int skierId = item.getInt("skierId");
        uniqueSkiers.add(skierId);
      }

      return uniqueSkiers.size();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error getting unique skiers count", e);
      return 0;
    }
  }
  /**
   * List all tables in DynamoDB to test connectivity
   */
  public List<String> listTables() {
    try {
      ListTablesResult result = client.listTables();
      return result.getTableNames();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error listing tables", e);
      throw e;
    }
  }
  /**
   * Shutdown the DynamoDB manager with graceful cleanup.
   */
  public void shutdown() {
    logger.info("Initiating DynamoDB manager shutdown...");

    // Process any remaining items
    logger.info("Processing remaining items in queue: " + queueSize.get());
    processBatch();

    // Shutdown executor services
    batchWriteService.shutdown();
    batchProcessService.shutdown();

    try {
      // Give more time for processing to complete
      if (!batchWriteService.awaitTermination(20, TimeUnit.SECONDS)) {
        batchWriteService.shutdownNow();
      }
      if (!batchProcessService.awaitTermination(20, TimeUnit.SECONDS)) {
        batchProcessService.shutdownNow();
      }
    } catch (InterruptedException e) {
      batchWriteService.shutdownNow();
      batchProcessService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Final stats
    logger.info("DynamoDB Manager Shutdown - Final Statistics:");
    reportStats();

    // Log success rate
    double successRate = failedWrites.get() > 0
        ? (double) totalWrites.get() / (totalWrites.get() + failedWrites.get()) * 100
        : 100.0;
    logger.info(String.format("Write Success Rate: %.2f%%", successRate));
  }

  private static class TokenBucket{
    private final int tokenPerSecond;
    private final int maxBucketSize;
    private final AtomicInteger availableToken;
    private final AtomicLong lastRefillTimestamp;

    public TokenBucket(int tokenPerSecond, int maxBucketSize) {
      this.tokenPerSecond = tokenPerSecond;
      this.maxBucketSize = maxBucketSize;
      this.availableToken = new AtomicInteger(maxBucketSize);
      this.lastRefillTimestamp = new AtomicLong(System.currentTimeMillis());
    }

    public boolean tryConsume(int tokenNum){
      refill();

      int currentToken = availableToken.get();
      if (currentToken>=tokenNum){
        return availableToken.compareAndSet(currentToken, currentToken-tokenNum);
      }
      return false;
    }

    /**
     * Wait until tokens are available and consume them
     */
    public void consumeWithBackoff(int tokens) throws InterruptedException {
      int backoffMs = 5; // Starting backoff time
      int maxBackoffMs = 1000; // Max backoff time

      while (true) {
        if (tryConsume(tokens)) {
          return; // Successfully consumed tokens
        }

        // Exponential backoff with cap
        Thread.sleep(backoffMs);
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }

    /**
     * Refill the token bucket based on elapsed time
     */
    public void refill() {
      long now = System.currentTimeMillis();
      long timeElapsed = now - lastRefillTimestamp.getAndSet(now);

      if (timeElapsed > 0) {
        // Calculate tokens to add based on time elapsed
        double tokensToAdd = (tokenPerSecond * timeElapsed) / 1000.0;

        if (tokensToAdd > 0) {
          int newTokens = (int)tokensToAdd;

          // Add tokens up to max bucket size
          int currentTokens;
          int updatedTokens;
          do {
            currentTokens = availableToken.get();
            updatedTokens = Math.min(currentTokens + newTokens, maxBucketSize);
          } while (!availableToken.compareAndSet(currentTokens, updatedTokens));
        }
      }
    }

    public int getAvailableTokens() {
      refill();
      return availableToken.get();
    }

  }
}