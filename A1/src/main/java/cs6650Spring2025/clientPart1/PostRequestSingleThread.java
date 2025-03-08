package cs6650Spring2025.clientPart1;

import cs6650Spring2025.clientPart2.PostRequestMetrics;
import cs6650Spring2025.clientPart2.RecordWriter;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletResponse;


public class PostRequestSingleThread implements Runnable {
  private final int numRequests;
  private final AtomicInteger counterSuccess;
  private final AtomicInteger counterFail;
  private final CountDownLatch countDownLatch;
  private final LiftRideEventRandomGenerator liftRideEventRandomGenerator;
  private final SkiersApi skiersApi;
//  private static final String BASE_PATH = "http://44.233.246.8:8080/A1_war/";
  private static final String BASE_PATH = "http://localhost:8080/A1_war_exploded";
  private static final int RETIRES_THRESHOLD = 5;
  private final int threadID;
  private final RecordWriter recordWriter;

  public PostRequestSingleThread(int numRequests, AtomicInteger counterSuccess,
      AtomicInteger counterFail, CountDownLatch countDownLatch,
      LiftRideEventRandomGenerator liftRideEventRandomGenerator, RecordWriter recordWriter) {
    this.numRequests = numRequests;
    this.counterSuccess = counterSuccess;
    this.counterFail = counterFail;
    this.countDownLatch = countDownLatch;
    this.liftRideEventRandomGenerator = liftRideEventRandomGenerator;
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(BASE_PATH);
    this.skiersApi = new SkiersApi(apiClient);
    this.threadID = ThreadLocalRandom.current().nextInt();
    this.recordWriter = recordWriter;
  }

  /**
   * @see Thread#run()
   */
  @Override
  public void run() {

    int completedRequest = 0;
    for (int i=0; i<numRequests ; i++) {
//      if (i % 100 == 0) { // Log every 100 requests
////        System.out.println("Thread " + threadID + " has completed " + i + " requests");
//      }
      boolean success = false;
      int retryCount = 0;
      long startTime = System.currentTimeMillis();
      while(!success && retryCount < RETIRES_THRESHOLD ) {
        try{
          LiftRideEvent liftRideEvent = liftRideEventRandomGenerator.getNextLiftRideEvent();
          LiftRide liftRide = new LiftRide();
          liftRide.setLiftID(liftRideEvent.getLiftID());
          liftRide.setTime(liftRideEvent.getTime());

//          System.out.println("Sending request to EC2: " + BASE_PATH +
//              "/skiers/" + liftRideEvent.getResortID() +
//              "/seasons/" + liftRideEvent.getSeasonID() +
//              "/days/" + liftRideEvent.getDayID() +
//              "/skiers/" + liftRideEvent.getSkierID());

          ApiResponse<Void> response = skiersApi.writeNewLiftRideWithHttpInfo(liftRide,
              liftRideEvent.getResortID(), liftRideEvent.getSeasonID(), liftRideEvent.getDayID(), liftRideEvent.getSkierID());

          long endTime = System.currentTimeMillis();
          recordWriter.addPostMetric(new PostRequestMetrics(startTime, endTime-startTime, "POST", response.getStatusCode()));

          success = true;
          completedRequest++;
          counterSuccess.incrementAndGet();



        } catch (ApiException e) {
          long endTime = System.currentTimeMillis();
          recordWriter.addPostMetric(new PostRequestMetrics(startTime, endTime-startTime, "POST", e.getCode()));
          if (e.getCode() >= HttpServletResponse.SC_BAD_REQUEST && e.getCode() <HttpServletResponse.SC_HTTP_VERSION_NOT_SUPPORTED && retryCount
              < (RETIRES_THRESHOLD-1) ){
            retryCount++;
          }else{

            counterFail.incrementAndGet();
            break;
          }

        }catch (Exception e){
          if (retryCount < (RETIRES_THRESHOLD-1)){
            retryCount++;
          }else{
            counterFail.incrementAndGet();
            break;
          }
        }
      }

    }
    countDownLatch.countDown();
  }

}
