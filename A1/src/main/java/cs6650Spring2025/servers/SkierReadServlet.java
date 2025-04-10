package cs6650Spring2025.servers;

import com.google.gson.Gson;

import cs6650Spring2025.assignment3Database.DynamoDBService;
import io.swagger.client.model.ResponseMsg;
import java.io.IOException;
import java.util.logging.Logger;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet dedicated to handling GET requests for skier data.
 */
public class SkierReadServlet extends HttpServlet {
  private DynamoDBService dynamoDBService;
  private static final Logger logger = Logger.getLogger(
      SkierReadServlet.class.getName());


  @Override
  public void init() throws ServletException {
    super.init();

    try {
      dynamoDBService = new DynamoDBService();
      logger.info("SkierReadServlet initialized successfully with DynamoDBService");
    } catch (Exception e) {
      logger.severe("SkierReadServlet initialized with exception: " + e.getMessage());
      throw new ServletException("Failed to initialize DynamoDBService", e);
    }
  }

  @Override
  public void destroy() {
    if (dynamoDBService != null) {
      dynamoDBService.shutdown();
      logger.info("SkierReadServlet destroyed successfully with DynamoDBService");
    }
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    String method = req.getMethod();
    if (method.equals("GET")) {
      doGet(req, res);
    } else {
      res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Only GET methods are supported by SkierReadServlet");
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
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

    // Check if the URL is a valid skier or resort URL
    boolean isValidUrl = isSkierUrlValid(urlParts) || isResortUrlValid(urlParts);


    if (!isValidUrl) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid URL pattern - first pass");
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
       else if (urlParts.length == 7) {
        // Handle /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
        handleResortDistinctSkier(req, res, urlParts);
      } else {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ResponseMsg errorMsg = new ResponseMsg();
        errorMsg.setMessage("Invalid URL pattern - SECOND PASSED " + urlParts);
        res.getWriter().write(new Gson().toJson(errorMsg));
      }
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
      logger.warning("Error in doGet: " + e.getMessage());
    }
  }

  /**
   * Handle GET/resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
   * @param req HTTP request
   * @param res HTTP response
   * @param urlParts URL parts from path
   */
  private void handleResortDistinctSkier(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");

    try {
      int resortId = Integer.parseInt(urlParts[1]);
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int uniqueSkiers = dynamoDBService.getNumberOfUniqueSkiers(resortId, seasonId, dayId);
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(uniqueSkiers));

    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (NullPointerException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  /**
   * Handle GET/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
   * @param req HTTP request
   * @param res HTTP response
   * @param urlParts URL parts from path
   */
  private void handleSkiersResortsVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");
    try {
      int resortId = Integer.parseInt(urlParts[1]);
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int skierId = Integer.parseInt(urlParts[7]);

      // Query DynamoDB for vertical totals
      int verticalTotal = dynamoDBService.getSkierVerticalForDay(skierId, seasonId, dayId);

      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(verticalTotal));
    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (NullPointerException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  /**
   * Handle GET/skiers/{skierID}/vertical
   * @param req HTTP request
   * @param res HTTP response
   * @param urlParts URL parts from path
   */
  private void handleSkiersResortsTotalVertical(HttpServletRequest req, HttpServletResponse res, String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");

    try {
      int skierId = Integer.parseInt(urlParts[1]);

      if (!dynamoDBService.hasSkierData(skierId)) {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        ResponseMsg errorMsg = new ResponseMsg();
        errorMsg.setMessage("No vertical data found for skier ID: " + skierId);
        res.getWriter().write(new Gson().toJson(errorMsg));
        return;
      }

      int totalVertical = dynamoDBService.getTotalSkierVertical(skierId);

      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(totalVertical));
    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid skier ID format: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Server error: " + e.getMessage());
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  /**
   * Validate URL for skier endpoints
   * @param urlParts URL parts from path
   * @return true if valid, false otherwise
   */
  private boolean isSkierUrlValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    // skiers/{skierID}/vertical
    if (urlParts.length == 3) {
      try {
        Integer.parseInt(urlParts[1]); // Validate skierId
        return urlParts[2].equals("vertical");
      } catch (NumberFormatException e) {
        return false;
      }
    }

    // skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);
        Integer.parseInt(urlParts[7]);
        int dayId = Integer.parseInt(urlParts[5]);

        return urlParts[2].equals("seasons") &&
            urlParts[4].equals("days") &&
            urlParts[6].equals("skiers") &&
            dayId >= 1 && dayId <= 3;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    return false;
  }

  /**
   * Validate URL for resort endpoints
   * @param urlParts URL parts from path
   * @return true if valid, false otherwise
   */
  private boolean isResortUrlValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    // /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
    if (urlParts.length == 7) {
      try {
        Integer.parseInt(urlParts[1]);
        Integer.parseInt(urlParts[5]);
        int dayId = Integer.parseInt(urlParts[5]);

        return urlParts[2].equals("seasons") &&
            urlParts[4].equals("day") &&
            urlParts[6].equals("skiers") &&
            dayId >= 1 && dayId <= 3;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    return false;
  }
}