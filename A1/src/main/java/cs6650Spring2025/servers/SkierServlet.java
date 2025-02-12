package cs6650Spring2025.servers;

import com.google.gson.Gson;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.ResponseMsg;
import io.swagger.client.model.SkierVertical;
import io.swagger.client.model.SkierVerticalResorts;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//@WebServlet(value = "/skiers/*")  // URL pattern for this servlet
public class SkierServlet extends HttpServlet {

@Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    // check we have a URL!
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
    //
    try{
      //1. handle  /skiers/{skierID}/vertical
      if (urlParts[2].equals("vertical")){
        handleSkiersResortsTotalVertical(req, res, Integer.parseInt(urlParts[1]));
      }else{
        // Handle /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        handleSkiersResortsVertical(req, res, urlParts);
      }
  }catch (Exception e){
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found");
      res.getWriter().write(new Gson().toJson(errorMsg));
  }
  }

  /**
   * get ski day vertical for a skier
   * URL path:  /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
   * @param req
   * @param res
   */
  private void handleSkiersResortsVertical(HttpServletRequest req, HttpServletResponse res,  String[] urlParts) throws ServletException, IOException {
    res.setContentType("application/json");
    try{
      String resortId = urlParts[1];
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int skierId = Integer.parseInt(urlParts[7]);

      //TODO: temporary number after getting the database connection
      int verticalTotal = 34507;
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(String.valueOf(verticalTotal));

    }catch (NumberFormatException e){
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied");
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (NullPointerException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs supplied");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }catch (Exception e){
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Data Not Found");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  /**
   * get the total vertical for the skier for specified seasons at the specified resort
   * @param req
   * @param res
   */
  private void handleSkiersResortsTotalVertical(HttpServletRequest req, HttpServletResponse res, Integer skierId) throws ServletException, IOException {

    String resortQuery = req.getParameter("resort");
    String seasonQuery = req.getParameter("season");

    if (resortQuery == null || resortQuery.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Missing required resort parameter");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    // Create and return SkierVertical object
    SkierVertical skierVertical = new SkierVertical();
    List<SkierVerticalResorts> verticalResortsList = new ArrayList<>();

    if (seasonQuery != null && !seasonQuery.isEmpty()) {
      SkierVerticalResorts skierVerticalResorts = getVerticalResortBySeason(skierId, resortQuery, seasonQuery);
      if (skierVerticalResorts != null) {
        verticalResortsList.add(skierVerticalResorts);
      }
    }else{
      List<SkierVerticalResorts> allSeasons = getAllVerticalResorts(skierId, resortQuery);
      if (allSeasons != null && allSeasons.size() > 0) {
        verticalResortsList.addAll(allSeasons);
      }
      if (verticalResortsList.size() == 0) {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        ResponseMsg errorMsg = new ResponseMsg();
        errorMsg.setMessage("Data Not Found");
        res.getWriter().write(new Gson().toJson(errorMsg));
        return;
      }
      skierVertical.setResorts(verticalResortsList);
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(new Gson().toJson(skierVertical));
    }
    // Add resort totals - replace with actual data
    res.setStatus(HttpServletResponse.SC_OK);
    res.getWriter().write(new Gson().toJson(skierVertical));
  }


  // TODO: Implement database query to get vertical for all seasons
  private List<SkierVerticalResorts> getAllVerticalResorts(Integer skierId, String resortQuery) {
    List<SkierVerticalResorts> resorts = new ArrayList<>();

    SkierVerticalResorts resort = new SkierVerticalResorts();
//    resort.setSeasonID("2025");
//    resort.setTotalVert(34507);
//    resorts.add(resort);

    return resorts;
  }

  // TODO: Implement database query to get vertical for specific season
  private SkierVerticalResorts getVerticalResortBySeason(Integer skierId, String resortQuery, String seasonQuery) {
    SkierVerticalResorts resort = new SkierVerticalResorts();
//    resort.setSeasonID(seasonQuery);
//    resort.setTotalVert(34507);
    return resort;
  }

  private boolean isSkierUrlValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    // Valid patterns:
    // /skiers/{skierID}/vertical
    if (urlParts.length == 3) {
      try {
        Integer.parseInt(urlParts[1]); // Validate skierId
        return urlParts[2].equals("vertical");
      } catch (NumberFormatException e) {
        return false;
      }
    }
    // /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);


        Integer.parseInt(urlParts[7]);// Validate resortID is numeric
        // Validate URL structure
        return urlParts[2].equals("seasons")&&
            urlParts[4].equals("days") &&
            urlParts[6].equals("skiers") && Integer.parseInt(urlParts[5])>=1 && Integer.parseInt(urlParts[5])<=366;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {

//    System.out.println("============ POST REQUEST RECEIVED ============");
//    System.out.println("Request URI: " + req.getRequestURI());
//    System.out.println("Context Path: " + req.getContextPath());
//    System.out.println("Servlet Path: " + req.getServletPath());
//    System.out.println("Path Info: " + req.getPathInfo());
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();


    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Missing required parameters");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)

    if (!isPostSkierURLValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }
    try{
      StringBuilder sb = new StringBuilder();
      BufferedReader reader = req.getReader();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }

      Gson gson = new Gson();
      LiftRide liftRide = gson.fromJson(sb.toString(), LiftRide.class);

      int resortId = Integer.parseInt(urlParts[1]);
      String seasonId = urlParts[3];
      String dayId = urlParts[5];
      int skierId = Integer.parseInt(urlParts[7]);
      handleWriteNewLiftRide(res, liftRide, resortId, seasonId, dayId, skierId);

    }catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid input");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  private void handleWriteNewLiftRide(HttpServletResponse res, LiftRide liftRide, int resortId, String seasonId, String dayId, int skierId) throws IOException {
    if (liftRide == null || liftRide.getLiftID()==null || liftRide.getTime()==null) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid lift ride data");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    res.setStatus(HttpServletResponse.SC_CREATED);
  }

  private boolean isPostSkierURLValid(String[] urlParts) {
    if (urlParts == null || urlParts.length == 0) return false;

    // /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);

        Integer.parseInt(urlParts[7]);// Validate resortID is numeric
        // Validate URL structure
        return urlParts[2].equals("seasons")&&
            urlParts[4].equals("days") &&
            urlParts[6].equals("skiers") && Integer.parseInt(urlParts[5])>=1 && Integer.parseInt(urlParts[5])<=366;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }
}