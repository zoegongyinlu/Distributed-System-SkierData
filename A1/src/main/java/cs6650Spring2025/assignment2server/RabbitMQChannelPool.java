package cs6650Spring2025.assignment2server;


import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cs6650Spring2025.assignment3Database.SkiResortDynamoDBManager;
import cs6650Spring2025.clientPart1.LiftRideEvent;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;
import io.swagger.client.model.SkierVerticalResorts;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Manager class for the RabbitMQ channel pool.
 * https://github.com/gortonator/foundations-of-scalable-systems/blob/main/Ch7/rmqpool/RMQChannelPool.java
 */
public class RabbitMQChannelPool {
  private final GenericObjectPool<Channel> channelPool;
  private final int MIN_POOL_SIZE = 1;
  private int MAX_POOL_SIZE = 50;

  private final String queueNamePrefix;
  private Connection connection;

  //TODO: adjust
  private final int RECOVERY_DELAY = 50000;
  private final int RECOVERY_HEART_BEAT = 60;

  public RabbitMQChannelPool(String host, int port,
      String username,
      String password,
      String queueNamePrefix, int numQueues,
      int poolSize) throws IOException, TimeoutException {

    this.queueNamePrefix = queueNamePrefix;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);

    factory.setAutomaticRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(RECOVERY_DELAY);
    factory.setRequestedHeartbeat(RECOVERY_HEART_BEAT);
    factory.setConnectionTimeout(15000);
    factory.setHandshakeTimeout(10000);

    this.connection = factory.newConnection();

    ChannelFactory channelFactory = new ChannelFactory(connection, queueNamePrefix, numQueues);


    GenericObjectPoolConfig<Channel> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
    genericObjectPoolConfig.setMaxTotal(MAX_POOL_SIZE);
    genericObjectPoolConfig.setMaxIdle(MAX_POOL_SIZE);
    genericObjectPoolConfig.setMinIdle(MIN_POOL_SIZE);
    genericObjectPoolConfig.setBlockWhenExhausted(true);
    genericObjectPoolConfig.setTestOnBorrow(true);
    genericObjectPoolConfig.setTestOnReturn(true);
    genericObjectPoolConfig.setJmxEnabled(false);

    this.channelPool = new GenericObjectPool<>(channelFactory, genericObjectPoolConfig);

  }

  /**
   * Borrows a channel from the pool.
   *
   * @return a channel from the pool
   * @throws Exception if there's an error borrowing from the pool
   */
  public Channel borrowChannel() throws Exception {

    return channelPool.borrowObject();
  }

  /**
   * Returns a channel to the pool.
   *
   * @param channel the channel to return
   */

  public void returnChannel(Channel channel) {
    if (channel != null) {
      channelPool.returnObject(channel);
    }
  }

  public void returnChannelIfInvalid(Channel channel, boolean invalid) {
    if (channel != null) {
      if (invalid) {
        try{
          channelPool.invalidateObject(channel);
        }catch (Exception e){
          System.err.println("Error invalidating channel: " + e.getMessage());
        }
      }else {
        channelPool.returnObject(channel);
      }
    }
  }

  public void closePool() throws IOException {
    channelPool.close();

    if (connection != null && connection.isOpen()) {
      connection.close();
    }
  }

  /**
   * Gets queue name based on a key value for distributing messages.
   *
   * @param skierID The value to use for determining queue (e.g., skierId)
   * @param numQueues Total number of queues
   * @return The appropriate queue name
   */
  public String getQueueName(int skierID, int numQueues) {
    int queueIndex = skierID % numQueues;
    return queueNamePrefix + queueIndex;
  }


}
