package cs6650Spring2025.assignment2server;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * A factory for creating and managing RabbitMQ channels using Apache Commons Pool .
 * https://github.com/gortonator/foundations-of-scalable-systems/blob/main/Ch7/rmqpool/RMQChannelFactory.java
 */
public class ChannelFactory extends BasePooledObjectFactory<Channel>  {
  private final Connection connection;
  private int count;
  private final String queNamePrefix;
  private final int numQueues;

  public ChannelFactory(Connection connection, String queNamePrefix, int numQueues) {
    this.connection = connection;
    this.queNamePrefix = queNamePrefix;
    this.numQueues = numQueues;

  }
  @Override
  public Channel create() throws Exception {
    Channel channel = connection.createChannel();
    //durable: true, exclusive: false, autodelete:false

    for (int i = 0; i < numQueues; i++) {
      String queueName = queNamePrefix + i;
      channel.queueDeclare(queueName, true, false, false, null);
    }


    return channel;
  }
  /**
   * Wraps the Channel in a DefaultPooledObject for the pool to manage.
   *
   * @param channel the channel to wrap
   * @return the wrapped channel
   */
  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }
  /**
   * Validates if a channel is still valid for use.
   * @param p the pooled object to validate
   * @return true if the channel is valid, false otherwise
   */

  public boolean validateObject(PooledObject<Channel> p) {
    Channel channel = p.getObject();
    return channel != null && channel.isOpen();
  }

  public void destroyObject(PooledObject<Channel> p) throws Exception {
    Channel channel = p.getObject();
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
  }
}
