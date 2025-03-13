package cs6650Spring2025.assignment2server;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.io.IOException;

@WebListener
public class ApplicationShutDownListener implements ServletContextListener {
  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    // Get the RabbitMQChannelPool instance from your application context
    RabbitMQChannelPool channelPool = (RabbitMQChannelPool) sce.getServletContext()
        .getAttribute("rabbitmqChannelPool");

    if (channelPool != null) {
      try {
        channelPool.closePool();
        System.out.println("Successfully closed RabbitMQ channel pool");
      } catch (IOException e) {
        System.err.println("Error closing RabbitMQ channel pool: " + e.getMessage());
      }
    }
  }

}