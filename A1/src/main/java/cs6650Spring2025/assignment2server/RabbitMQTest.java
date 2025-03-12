//package cs6650Spring2025.assignment2server;
//
//import com.rabbitmq.client.Channel;
//import com.rabbitmq.client.Connection;
//import com.rabbitmq.client.ConnectionFactory;
//
//import java.nio.charset.StandardCharsets;
//
//public class RabbitMQTest {
//  private static final String QUEUE_NAME = "skier_lift_ride_queue";
//  private static final String RABBITMQ_HOST = "54.70.80.157";
//  private static final int RABBITMQ_PORT = 5672;
//  private static final String RABBITMQ_USERNAME = "admin";
//  private static final String RABBITMQ_PASSWORD = "654321";
//
//  public static void main(String[] args) {
//    try {
//      // Create connection factory
//      ConnectionFactory factory = new ConnectionFactory();
//      factory.setHost(RABBITMQ_HOST);
//      factory.setPort(RABBITMQ_PORT);
//      factory.setUsername(RABBITMQ_USERNAME);
//      factory.setPassword(RABBITMQ_PASSWORD);
//
//      System.out.println("Connecting to RabbitMQ...");
//
//      // Create connection
//      try (Connection connection = factory.newConnection();
//          Channel channel = connection.createChannel()) {
//
//        System.out.println("Connected to RabbitMQ successfully!");
//
//        // Declare queue
//        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
//        System.out.println("Queue declared: " + QUEUE_NAME);
//
//        // Create a test message
//        String message = "Test message " + System.currentTimeMillis();
//
//        // Publish message
//        channel.basicPublish("", QUEUE_NAME, null,
//            message.getBytes(StandardCharsets.UTF_8));
//
//        System.out.println("Sent message: " + message);
//      }
//    } catch (Exception e) {
//      System.err.println("Error connecting to RabbitMQ: " + e.getMessage());
//      e.printStackTrace();
//    }
//  }
//}