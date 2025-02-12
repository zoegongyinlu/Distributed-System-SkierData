package cs6650Spring2025.servers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.swagger.client.model.ResortIDSeasonsBody;
import io.swagger.client.model.ResortSkiers;
import io.swagger.client.model.ResortsList;
import io.swagger.client.model.ResortsListResorts;
import io.swagger.client.model.ResponseMsg;
import io.swagger.client.model.SeasonsList;
import java.io.BufferedReader;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;

//TODO: Incomplete
@WebServlet(value = "/resorts/*")
public class ResortServlet extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    // handle: "/resorts/"
    if (urlPath == null || urlPath.isEmpty()) {
      handleGetResorts(res);
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isResortUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write("{\"message\": \"Invalid URL format\"}");
      return;
    }

    // Handle different GET endpoints
    try {
      if (urlParts.length == 3 && urlParts[2].equals("seasons")) {
        // for: /resorts/{resortID}/seasons
        handleGetResortSeasons(res, Integer.parseInt(urlParts[1]));
      } else if (urlParts.length == 7 && urlParts[2].equals("seasons") && urlParts[4].equals("day") && urlParts[6].equals("skiers")) {
        // Handle /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
        handleGetResortSkiersDay(res, Integer.parseInt(urlParts[1]),
            Integer.parseInt(urlParts[3]),
            Integer.parseInt(urlParts[5]));
      } else {
        res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        res.getWriter().write("{\"message\": \"Invalid URL pattern\"}");
      }
    } catch (NumberFormatException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      res.getWriter().write("{\"message\": \"Invalid number format in URL\"}");
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("{\"message\": \"Missing parameters\"}");
      return;
    }

    String[] urlParts = urlPath.split("/");

    // Handle POST /resorts/{resortID}/seasons
    if (urlParts.length == 3 && urlParts[2].equals("seasons")) {
      try {
        // Read the request body
        StringBuilder sb = new StringBuilder();
        String line;
        BufferedReader reader = req.getReader();
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        res.setStatus(HttpServletResponse.SC_OK);
        // Handle adding new season
        handleAddSeason(res, Integer.parseInt(urlParts[1]), sb.toString());
      } catch (Exception e) {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        res.getWriter().write("{\"message\": \"Invalid inputs\"}");
      }
    } else {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("{\"message\": \"Resort not found\"}");
    }
  }

  private void handleGetResorts(HttpServletResponse res) throws IOException {
    // Return list of resorts
    res.setContentType("application/json");
    try{
      ResortsList resortsList = new ResortsList();
      List<ResortsListResorts> resorts = new ArrayList<>();
      //TODO: for interacting with database using either sql or SELECT ...


      resortsList.setResorts(resorts);
      Gson gson = new Gson();
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write(gson.toJson(resorts));


    }catch (JsonSyntaxException e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Error retrieving resort list");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }
  }

  private void handleGetResortSeasons(HttpServletResponse res, int resortId) throws IOException {
    if (!checkResortByResortId(resortId)){
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Resort not found");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }
    res.setStatus(HttpServletResponse.SC_OK);
    SeasonsList seasonsList = getSeasonsByResortId(resortId);
    res.getWriter().write(new Gson().toJson(seasonsList));
  }
 // TODO: Complete this
  private SeasonsList getSeasonsByResortId(int resortId) {
    return null;
  }
//TODO:
  /**
   * Return boolean if the resort exists by checking the resortId
   * @param resortId
   * @return
   */
  private boolean checkResortByResortId(int resortId) {
    return true;
  }

  private void handleGetResortSkiersDay(HttpServletResponse res, int resortId, int seasonId, int dayId) throws IOException {
    // Return number of skiers
    if (!checkResortByResortId(resortId)){
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Resort not found");
      res.getWriter().write(new Gson().toJson(errorMsg));
      return;
    }

    res.setStatus(HttpServletResponse.SC_OK);
    ResortSkiers resortSkiers = getResortSkiersDayByResortIdSeasonIdDayId(resortId, seasonId, dayId);
    res.getWriter().write(new Gson().toJson(resortSkiers));

  }
//TODO:
  private ResortSkiers getResortSkiersDayByResortIdSeasonIdDayId(int resortId, int seasonId, int dayId) {
    return null;
  }

  private void handleAddSeason(HttpServletResponse res, int resortId, String seasonData) throws IOException {
    // Process adding new season

    res.setContentType("application/json");
    try{
      Gson gson = new Gson();
      ResortIDSeasonsBody seasonsBody = gson.fromJson(seasonData, ResortIDSeasonsBody.class);
      if (seasonsBody == null || seasonsBody.getYear()==null) {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ResponseMsg errMsg = new ResponseMsg();
        errMsg.setMessage("Invalid inputs");
        res.getWriter().write(gson.toJson(errMsg));
        return;
      }
      res.setStatus(HttpServletResponse.SC_CREATED);
      ResponseMsg successMsg = new ResponseMsg();
      successMsg.setMessage("new season created");
      res.getWriter().write(gson.toJson(successMsg));

    }  catch (JsonSyntaxException e) {
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Invalid inputs");
      res.getWriter().write(new Gson().toJson(errorMsg));
    } catch (Exception e) {
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ResponseMsg errorMsg = new ResponseMsg();
      errorMsg.setMessage("Error processing request");
      res.getWriter().write(new Gson().toJson(errorMsg));
    }

  }

  private boolean isResortUrlValid(String[] urlParts) {
    // Validate URL patterns
    if (urlParts == null || urlParts.length == 0) return false;

    // Valid patterns:
    // /resorts/{resortID}/seasons
    if (urlParts.length == 3) {
      try {
        Integer.parseInt(urlParts[1]); // Validate resortID is numeric
        return urlParts[2].equals("seasons");
      } catch (NumberFormatException e) {
        return false;
      }
    }

    // /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
    if (urlParts.length == 7) {
      try {
        Integer.parseInt(urlParts[1]);

       // Validate resortID is numeric
        // Validate URL structure
        return urlParts[2].equals("seasons") &&
            urlParts[4].equals("day") &&
            urlParts[6].equals("skiers") &&  Integer.parseInt(urlParts[5])>=1 &&  Integer.parseInt(urlParts[5])<=366;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    return false;
  }
}
