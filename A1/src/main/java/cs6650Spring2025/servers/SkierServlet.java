package cs6650Spring2025.servers;

import com.google.gson.Gson;

import com.rabbitmq.client.Channel;
import cs6650Spring2025.assignment2server.MessageQueueService;

import cs6650Spring2025.assignment3Database.DynamoDBService;

import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.logging.Logger;

public class SkierServlet extends HttpServlet {


  private MessageQueueService messageQueueService;
  private DynamoDBService dynamoDBService;
  private static final int NUMBER_OF_QUEUES = 7;
  private static final Logger logger = Logger.getLogger(
      SkierServlet.class.getName());


  @Override
  public void init() throws ServletException {
    super.init();

    try {
      // Set up RabbitMQ channel pool
      messageQueueService = new MessageQueueService(NUMBER_OF_QUEUES);
      dynamoDBService = new DynamoDBService();


      // Store in servlet context for potential use by other servlets
      getServletContext().setAttribute("messageQueueService", messageQueueService);
      getServletContext().setAttribute("dynamoDBService", dynamoDBService);
      logger.info("SkierServlet initialized successfully with MessageQueueService and DynamoDBService");



    } catch (Exception e) {
      throw new ServletException("Failed to initialize resources", e);
    }
  }

  @Override
  public void destroy() {
    try {
      if (messageQueueService != null) {
        messageQueueService.shutdown();
      }

      if (dynamoDBService != null) {
        dynamoDBService.shutdown();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {

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

      boolean publishSuccess = messageQueueService.publishLiftRideEvent(skierId, resortId, seasonId, dayId, liftRide);

      if (publishSuccess) {
        res.setStatus(HttpServletResponse.SC_CREATED);
        ResponseMsg successMsg = new ResponseMsg();
        successMsg.setMessage("Lift ride event created successfully");
        res.getWriter().write(new Gson().toJson(successMsg));
      }else{
        sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Failed to publish message");
      }
    }
      catch (NumberFormatException e) {
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


//  @Override
//  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
//    res.setContentType("application/json");
//    String urlPath = req.getPathInfo();
//
//    if (urlPath == null || urlPath.isEmpty()) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Data Not Found");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//      return;
//    }
//
//    String[] urlParts = urlPath.split("/");
//
//    if (!isSkierUrlValid(urlParts)) {
//      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Invalid inputs");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//      return;
//    }
//
//    try {
//      if (urlParts.length == 3 && urlParts[2].equals("vertical")) {
//        // Handle /skiers/{skierID}/vertical
//        handleSkiersResortsTotalVertical(req, res, urlParts);
//      } else if (urlParts.length == 8) {
//        // Handle /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
//        handleSkiersResortsVertical(req, res, urlParts);
//      }
//    } catch (Exception e) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Data Not Found");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//      logger.warning("Error in doGet: " + e.getMessage());
//    }
//  }
//
//  private void handleSkiersResortsVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
//    res.setContentType("application/json");
//    try {
//      int resortId = Integer.parseInt(urlParts[1]);
//      String seasonId = urlParts[3];
//      String dayId = urlParts[5];
//      int skierId = Integer.parseInt(urlParts[7]);
//
//      // Query DynamoDB for vertical totals
//      int verticalTotal = dynamoDBService.getSkierVerticalForDay(skierId, seasonId, dayId);
//
//      res.setStatus(HttpServletResponse.SC_OK);
//      res.getWriter().write(String.valueOf(verticalTotal));
//    } catch (NumberFormatException e) {
//      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Invalid inputs supplied");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//    } catch (NullPointerException e) {
//      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Invalid inputs supplied");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//    } catch (Exception e) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Data Not Found");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//    }
//  }
//
//  private void handleSkiersResortsTotalVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
//    res.setContentType("application/json");
//
//    try {
//      int skierId = Integer.parseInt(urlParts[1]);
//
//      if (!dynamoDBService.hasSkierData(skierId)) {
//        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//        ResponseMsg errorMsg = new ResponseMsg();
//        errorMsg.setMessage("No vertical data found for skier ID: " + skierId);
//        res.getWriter().write(new Gson().toJson(errorMsg));
//        return;
//      }
//
//      int totalVertical = dynamoDBService.getTotalSkierVertical(skierId);
//
//      res.setStatus(HttpServletResponse.SC_OK);
//      res.getWriter().write(String.valueOf(totalVertical));
//    } catch (NumberFormatException e) {
//      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Invalid skier ID format");
//      res.getWriter().write(new Gson().toJson(errorMsg));
//    } catch (Exception e) {
//      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//      ResponseMsg errorMsg = new ResponseMsg();
//      errorMsg.setMessage("Server error: " + e.getMessage());
//      res.getWriter().write(new Gson().toJson(errorMsg));
//    }
//  }
//
//  private List<SkierVerticalResorts> getAllVerticalResorts(Integer skierId, String resortQuery) {
//    List<SkierVerticalResorts> resorts = new ArrayList<>();
//    return resorts;
//  }
//
//  private SkierVerticalResorts getVerticalResortBySeason(Integer skierId, String resortQuery, String seasonQuery) {
//    SkierVerticalResorts resort = new SkierVerticalResorts();
//    return resort;
//  }

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
