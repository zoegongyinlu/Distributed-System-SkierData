package cs6650Spring2025.assignment3Database;

import cs6650Spring2025.util.ConfigReader;

import java.util.Map;
import java.util.logging.Logger;

public class DynamoDBService {
  // DynamoDB configuration
  private static final String DYNAMODB_REGION;
  private static final String AWS_ACCESS_KEY;
  private static final String AWS_SECRET_KEY;

  private static final Logger logger = Logger.getLogger(DynamoDBService.class.getName());
  private SkiResortDynamoDBManager skiResortDynamoDBManager;

  static {
    ConfigReader.loadProperties();
    DYNAMODB_REGION = ConfigReader.getProperty("aws.dynamodb.region", "us-east-2");
    AWS_ACCESS_KEY = ConfigReader.getProperty("aws.access.key", "");
    AWS_SECRET_KEY = ConfigReader.getProperty("aws.secret.key", "");
  }

  public DynamoDBService() {
    initializeDynamoDBManager();
  }

  private void initializeDynamoDBManager() {
    try {
      skiResortDynamoDBManager = new SkiResortDynamoDBManager(DYNAMODB_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY);
      logger.info("DynamoDB manager initialized successfully");
    } catch (Exception e) {
      logger.severe("Failed to initialize DynamoDB manager: " + e.getMessage());
      throw new RuntimeException("Failed to initialize DynamoDB resources", e);
    }
  }


  /**
   * Get the vertical total for a skier on a specific day in a specific season
   */
  public int getSkierVerticalForDay(int skierId, String seasonId, String dayId) {
    try {
      Map<String, Integer> verticalByDay = skiResortDynamoDBManager.getSkierVerticalByDay(skierId);
      String dayKey = seasonId + "_" + dayId;
      return verticalByDay.getOrDefault(dayKey, 0);
    } catch (Exception e) {
      logger.warning("Error getting skier vertical for day: " + e.getMessage());
      return 0;
    }
  }

  public int getNumberOfUniqueSkiers(int resortId, String seasonId, String dayId){
    try{
      int res = skiResortDynamoDBManager.getUniqueSkiersCount(resortId, seasonId, dayId);
      return res;
    }catch (Exception e){
      logger.warning("Error getting unique skier count: " + e.getMessage());
      return 0;
    }
  }

  /**GET/skiers/{skierID}/vertical
   * Get the total vertical for a skier across all days and seasons
   * @param skierId The skier ID
   * @return The total vertical or 0 if not found
   */
  public int getTotalSkierVertical(int skierId) {
    try {
      Map<String, Integer> verticalByDay = skiResortDynamoDBManager.getSkierVerticalByDay(skierId);
      if (verticalByDay.isEmpty()) {
        return 0;
      }

      int totalVertical = 0;
      for (Integer vertical : verticalByDay.values()) {
        totalVertical += vertical;
      }
      return totalVertical;
    } catch (Exception e) {
      logger.warning("Error getting total skier vertical: " + e.getMessage());
      return 0;
    }
  }

  /**
   * Check if the skier has any vertical data
   */
  public boolean hasSkierData(int skierId) {
    try {
      Map<String, Integer> verticalByDay = skiResortDynamoDBManager.getSkierVerticalByDay(skierId);
      return !verticalByDay.isEmpty();
    } catch (Exception e) {
      logger.warning("Error checking if skier has data: " + e.getMessage());
      return false;
    }
  }



  public void shutdown() {
    if (skiResortDynamoDBManager != null) {
      skiResortDynamoDBManager.shutdown();
      com.amazonaws.http.IdleConnectionReaper.shutdown();
      logger.info("DynamoDB manager shut down successfully");
    }
  }
}