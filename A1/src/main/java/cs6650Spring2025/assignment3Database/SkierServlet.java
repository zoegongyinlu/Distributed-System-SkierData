package cs6650Spring2025.assignment3Database;

import com.google.gson.Gson;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import cs6650Spring2025.assignment2server.RabbitMQChannelPool;
import cs6650Spring2025.clientPart1.LiftRideEvent;
import cs6650Spring2025.util.ConfigReader;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;
import io.swagger.client.model.SkierVerticalResorts;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.logging.Logger;

//@WebServlet(value = "/skiers/*")  // URL pattern for this servlet
public class SkierServlet extends HttpServlet {
  static{
    ConfigReader.loadProperties();
  }
  private static final String QUEUE_NAME_PREFIX = "lift_ride_queue_";
  private final int NUMBER_OF_QUEUES = 7;
    private static final String RABBITMQ_HOST = ConfigReader.getProperty("rabbitmq.host", "54.70.80.157");
//  private static final String RABBITMQ_HOST = "localhost";
  private static final int RABBITMQ_PORT = ConfigReader.getIntProperty("rabbitmq.port", 5672);
  private static final String RABBITMQ_USERNAME = ConfigReader.getProperty("rabbitmq.username", "");
  private static final String RABBITMQ_PASSWORD = ConfigReader.getProperty("rabbitmq.password", "");

  // DynamoDB configuration
  private static final String DYNAMODB_REGION = ConfigReader.getProperty("aws.dynamodb.region", "us-east-2");
  private static final String AWS_ACCESS_KEY = ConfigReader.getProperty("aws.access.key", "");
  private static final String AWS_SECRET_KEY = ConfigReader.getProperty("aws.secret.key", "");

  private static final Logger logger = Logger.getLogger(
      cs6650Spring2025.assignment3Database.SkierServlet.class.getName());
  private static final int CHANNEL_POOL_SIZE = 25;

  private Connection rabbitConnection;
  private RabbitMQChannelPool rabbitMQChannelPool;
  private SkiResortDynamoDBManager skiResortDynamoDBManager; // Added DynamoDB manager

  @Override
  public void init() throws ServletException {
    super.init();

    try {
      // Set up RabbitMQ channel pool
      rabbitMQChannelPool = new RabbitMQChannelPool(
          RABBITMQ_HOST,
          RABBITMQ_PORT,
          RABBITMQ_USERNAME,
          RABBITMQ_PASSWORD,
          QUEUE_NAME_PREFIX,
          NUMBER_OF_QUEUES,
          CHANNEL_POOL_SIZE);

      // Store in servlet context for potential use by other servlets
      getServletContext().setAttribute("rabbitmqChannelPool", rabbitMQChannelPool);

      Object existingManager = getServletContext().getAttribute("dynamoDBManager");

      if (existingManager instanceof SkiResortDynamoDBManager) {
        skiResortDynamoDBManager = (SkiResortDynamoDBManager) existingManager;
        logger.info("Reusing existing DynamoDB manager from servlet context");
      }else{
        skiResortDynamoDBManager = new SkiResortDynamoDBManager(DYNAMODB_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY);
        getServletContext().setAttribute("dynamoDBManager", skiResortDynamoDBManager);
        logger.info("Created new DynamoDB manager and stored in servlet context");
      }

      // Test DynamoDB connectivity
      testDynamoDBConnection();


    } catch (Exception e) {
      throw new ServletException("Failed to initialize resources", e);
    }
  }

  /**
   * Helper function test the db connection
   */
  private void testDynamoDBConnection() {
    try {
      logger.info("Testing DynamoDB connection...");

      // Print credential info (only first few chars for security)
      if (AWS_ACCESS_KEY != null) {
        logger.info("Access Key ID: " + AWS_ACCESS_KEY.substring(0, 5) + "...");
      }

      // Check if table exists - simple operation to test connectivity
      skiResortDynamoDBManager.listTables();
      logger.info("DynamoDB connection successful!");
    } catch (Exception e) {
      logger.severe("DynamoDB connection test failed: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void destroy() {
    try {
      if (rabbitMQChannelPool != null) {
        rabbitMQChannelPool.closePool();
      }
      if (skiResortDynamoDBManager != null) {
        skiResortDynamoDBManager.shutdown();
        com.amazonaws.http.IdleConnectionReaper.shutdown();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
//        System.out.println("============ POST REQUEST RECEIVED ============");
//        System.out.println("Request URI: " + req.getRequestURI());
//        System.out.println("Context Path: " + req.getContextPath());
//        System.out.println("Servlet Path: " + req.getServletPath());
//        System.out.println("Path Info: " + req.getPathInfo());
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();
    String requestBody = null;
    Channel channel = null;
    boolean channelInvalid = false;

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      sendErrorResponse(res, HttpServletResponse.SC_NOT_FOUND, "Missing required URL path");
      return;
    }

    String[] urlParts = urlPath.split("/");
    if (!isPostSkierURLValid(urlParts)) {
      sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid URL format");
      return;
    }

    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader reader = req.getReader();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      requestBody = sb.toString();

      if (requestBody == null || requestBody.trim().isEmpty()) {
        sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Request body is empty");
        return;
      }

      Gson gson = new Gson();
      LiftRide liftRide = null;

      try {
        liftRide = gson.fromJson(requestBody, LiftRide.class);
      } catch (Exception e) {
        sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid JSON format: " + e.getMessage());
        return;
      }

      if (liftRide == null || liftRide.getLiftID() == null || liftRide.getTime() == null) {
        sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid lift ride data: missing required fields");
        return;
      }

      int resortId = Integer.parseInt(urlParts[1]);
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int skierId = Integer.parseInt(urlParts[7]);

      LiftRideEvent liftRideEvent = new LiftRideEvent(
          skierId,
          resortId,
          liftRide.getLiftID(),
          seasonId,
          dayId,
          liftRide.getTime()
      );
      String messageBody = gson.toJson(liftRideEvent);

      try {
        channel = rabbitMQChannelPool.borrowChannel();
        channel.basicPublish(
            "",
            rabbitMQChannelPool.getQueueName(skierId, NUMBER_OF_QUEUES),
            null,
            messageBody.getBytes(StandardCharsets.UTF_8)
        );

        res.setStatus(HttpServletResponse.SC_CREATED);
        ResponseMsg successMsg = new ResponseMsg();
        successMsg.setMessage("Lift ride event created successfully");
        res.getWriter().write(new Gson().toJson(successMsg));
      } catch (Exception e) {
        channelInvalid = true;
        sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to publish message: " + e.getMessage());
      } finally {
        if (channel != null) {
          rabbitMQChannelPool.returnChannelIfInvalid(channel, channelInvalid);
        }
      }
    } catch (NumberFormatException e) {
      sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid numeric input: " + e.getMessage());
    } catch (Exception e) {
      sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error: " + e.getMessage());
      getServletContext().log("Error in doPost: ", e);
    }
  }

  private void sendErrorResponse(HttpServletResponse res, int responseCode, String msg) throws IOException {
    res.setStatus(responseCode);
    ResponseMsg errorMsg = new ResponseMsg();
    errorMsg.setMessage(msg);
    res.getWriter().write(new Gson().toJson(errorMsg));
  }

  private boolean isPostSkierURLValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);
        Integer.parseInt(urlParts[7]);
        return urlParts[2].equals("seasons") &&
            urlParts[4].equals("days") &&
            urlParts[6].equals("skiers") &&
            Integer.parseInt(urlParts[5]) >= 1 &&
            Integer.parseInt(urlParts[5]) <= 366;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

//  // New doGet implementation from the DynamoDB version
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isSkierUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    try {
      if (urlParts.length == 3 && urlParts[2].equals("vertical")) {
        // Handle /skiers/{skierID}/vertical
        handleSkiersResortsTotalVertical(req, res, urlParts);
      } else if (urlParts.length == 8) {
        // Handle /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        handleSkiersResortsVertical(req, res, urlParts);
      }
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found");
      res.getWriter().write(new Gson().toJson(errorMsg));
      logger.warning("Error in doGet: " + e.getMessage());
    }
  }

  private void handleSkiersResortsVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");
    try {
      int resortId = Integer.parseInt(urlParts[1]);
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int skierId = Integer.parseInt(urlParts[7]);

      // Query DynamoDB for vertical totals
      Map<String, Integer> verticalByDay = skiResortDynamoDBManager.getSkierVerticalByDay(skierId);
      String dayKey = seasonId + "_" + dayId;
      int verticalTotal = verticalByDay.getOrDefault(dayKey, 0);

      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(verticalTotal));
    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied");
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (NullPointerException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied");
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  private void handleSkiersResortsTotalVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");

    try {
      int skierId = Integer.parseInt(urlParts[1]);

      Map<String, Integer> verticalByDay = skiResortDynamoDBManager.getSkierVerticalByDay(skierId);
      if (verticalByDay.isEmpty()) {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        ResponseMsg errorMsg = new ResponseMsg();
        errorMsg.setMessage("No vertical data found for skier ID: " + skierId);
        res.getWriter().write(new Gson().toJson(errorMsg));
        return;
      }

      int totalVertical = 0;
      for (Integer vertical : verticalByDay.values()) {
        totalVertical += vertical;
      }
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(totalVertical));
    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid skier ID format");
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Server error: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  private List<SkierVerticalResorts> getAllVerticalResorts(Integer skierId, String resortQuery) {
    List<SkierVerticalResorts> resorts = new ArrayList<>();
    return resorts;
  }

  private SkierVerticalResorts getVerticalResortBySeason(Integer skierId, String resortQuery, String seasonQuery) {
    SkierVerticalResorts resort = new SkierVerticalResorts();
    return resort;
  }

  private boolean isSkierUrlValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    // /skiers/{skierID}/vertical
    if (urlParts.length == 3) {
      try {
        Integer.parseInt(urlParts[1]); // Validate skierId
        return urlParts[2].equals("vertical");
      } catch (NumberFormatException e) {
        return false;}}



// /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);
        Integer.parseInt(urlParts[7]);
        int dayId = Integer.parseInt(urlParts[5]);

        return urlParts[2].equals("seasons") &&
            urlParts[4].equals("days") &&
            urlParts[6].equals("skiers") &&
            dayId >= 1 && dayId <= 366;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }
}
//
//
