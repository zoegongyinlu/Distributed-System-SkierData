package cs6650Spring2025.assignment2server;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Manager class for the RabbitMQ channel pool.
 * https://github.com/gortonator/foundations-of-scalable-systems/blob/main/Ch7/rmqpool/RMQChannelPool.java
 */
public class RabbitMQChannelPool {
  private final GenericObjectPool<Channel> channelPool;
  private final int MIN_POOL_SIZE = 1;
  private int poolSize;

  private final String queueName;
  private Connection connection;

  private final int RECOVERY_DELAY = 5000;
  private final int RECOVERY_HEART_BEAT = 30;

  public RabbitMQChannelPool(String host, int port,
      String username,
      String password,
      String queueName,
      int poolSize) throws IOException, TimeoutException {

    this.queueName = queueName;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);

    factory.setAutomaticRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(RECOVERY_DELAY);
    factory.setRequestedHeartbeat(RECOVERY_HEART_BEAT);

    this.connection = factory.newConnection();

    ChannelFactory channelFactory = new ChannelFactory(connection, queueName);


    GenericObjectPoolConfig<Channel> genericObjectPoolConfig = new GenericObjectPoolConfig<>();
    genericObjectPoolConfig.setMaxTotal(poolSize);
    genericObjectPoolConfig.setMaxIdle(poolSize);
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
    System.out.println("Borrowing channel from pool...");

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

  public String getQueueName() {
    return queueName;
  }

}
