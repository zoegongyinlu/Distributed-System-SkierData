package cs6650Spring2025.assignment2server;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import cs6650Spring2025.assignment2server.RabbitMQChannelPool;
import cs6650Spring2025.clientPart1.LiftRideEvent;
import cs6650Spring2025.util.ConfigReader;
import io.swagger.client.model.LiftRide;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class MessageQueueService {
  private static final String QUEUE_NAME_PREFIX = "lift_ride_queue_";
  private final int NUMBER_OF_QUEUES;
  private static final String RABBITMQ_HOST;
  private static final int RABBITMQ_PORT;
  private static final String RABBITMQ_USERNAME;
  private static final String RABBITMQ_PASSWORD;
  private static final int CHANNEL_POOL_SIZE = 25;

  private static final Logger logger = Logger.getLogger(MessageQueueService.class.getName());
  private RabbitMQChannelPool rabbitMQChannelPool;

  static {
    ConfigReader.loadProperties();
    RABBITMQ_HOST = ConfigReader.getProperty("rabbitmq.host", "54.70.80.157");
    RABBITMQ_PORT = ConfigReader.getIntProperty("rabbitmq.port", 5672);
    RABBITMQ_USERNAME = ConfigReader.getProperty("rabbitmq.username", "");
    RABBITMQ_PASSWORD = ConfigReader.getProperty("rabbitmq.password", "");
  }

  public MessageQueueService(int numberOfQueues) {
    this.NUMBER_OF_QUEUES = numberOfQueues;
    initializeChannelPool();
  }

  private void initializeChannelPool() {
    try {
      rabbitMQChannelPool = new RabbitMQChannelPool(
          RABBITMQ_HOST,
          RABBITMQ_PORT,
          RABBITMQ_USERNAME,
          RABBITMQ_PASSWORD,
          QUEUE_NAME_PREFIX,
          NUMBER_OF_QUEUES,
          CHANNEL_POOL_SIZE);
      logger.info("RabbitMQ channel pool initialized successfully");
    } catch (Exception e) {
      logger.severe("Failed to initialize RabbitMQ channel pool: " + e.getMessage());
      throw new RuntimeException("Failed to initialize RabbitMQ resources", e);
    }
  }

  public boolean publishLiftRideEvent(int skierId, int resortId, String seasonId, String dayId, LiftRide liftRide) {
    Channel channel = null;
    boolean channelInvalid = false;

    try {
      LiftRideEvent liftRideEvent = new LiftRideEvent(
          skierId,
          resortId,
          liftRide.getLiftID(),
          seasonId,
          dayId,
          liftRide.getTime()
      );

      Gson gson = new Gson();
      String messageBody = gson.toJson(liftRideEvent);

      channel = rabbitMQChannelPool.borrowChannel();
      channel.basicPublish(
          "",
          rabbitMQChannelPool.getQueueName(skierId, NUMBER_OF_QUEUES),
          null,
          messageBody.getBytes(StandardCharsets.UTF_8)
      );

      return true;
    } catch (Exception e) {
      channelInvalid = true;
      logger.severe("Failed to publish message: " + e.getMessage());
      return false;
    } finally {
      if (channel != null) {
        rabbitMQChannelPool.returnChannelIfInvalid(channel, channelInvalid);
      }
    }
  }

  public RabbitMQChannelPool getChannelPool() {
    return rabbitMQChannelPool;
  }

  public void shutdown() throws IOException {
    if (rabbitMQChannelPool != null) {
      rabbitMQChannelPool.closePool();
      logger.info("RabbitMQ channel pool shut down successfully");
    }
  }
}