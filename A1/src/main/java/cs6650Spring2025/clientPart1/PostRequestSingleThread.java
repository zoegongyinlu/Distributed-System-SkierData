package cs6650Spring2025.clientPart1;

import com.squareup.okhttp.ConnectionPool;
import cs6650Spring2025.clientPart2.PostRequestMetrics;
import cs6650Spring2025.clientPart2.RecordWriter;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletResponse;


public class PostRequestSingleThread implements Runnable {
  private final int numRequests;
  private final AtomicInteger counterSuccess;
  private final AtomicInteger counterFail;
  private final CountDownLatch completionLatch;
  private final CountDownLatch partialLatch;
  private final boolean countForPartial;
  private final LiftRideEventRandomGenerator liftRideEventRandomGenerator;
  private final SkiersApi skiersApi;
  // without ELB:
//  private static final String BASE_PATH = "http://44.233.246.8:8080/A1_war";
// To load balancer connection:
  private static final String BASE_PATH = "http://servlet-ELB-422677375.us-west-2.elb.amazonaws.com/A1_war";
  //with localhost
//  private static final String BASE_PATH = "http://localhost:8080/A1_war_exploded";
  private static final int RETIRES_THRESHOLD = 3;
  private final int threadID;
  private final RecordWriter recordWriter;
  private final double PHASE1_PARTIAL_THRESHOLD = 0.2;

  public PostRequestSingleThread(int numRequests, AtomicInteger counterSuccess,
      AtomicInteger counterFail, CountDownLatch completionLatch,
      LiftRideEventRandomGenerator liftRideEventRandomGenerator, RecordWriter recordWriter) {
    this.numRequests = numRequests;
    this.counterSuccess = counterSuccess;
    this.counterFail = counterFail;
    this.completionLatch = completionLatch;
    this.partialLatch = null;
    this.countForPartial = false;
    this.liftRideEventRandomGenerator = liftRideEventRandomGenerator;
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(BASE_PATH);
    apiClient.setVerifyingSsl(false);
    //add for concurrent connection pool

    this.skiersApi = new SkiersApi(apiClient);
    this.threadID = ThreadLocalRandom.current().nextInt();
    this.recordWriter = recordWriter;
  }
  public PostRequestSingleThread(int numRequests, AtomicInteger counterSuccess,
      AtomicInteger counterFail, CountDownLatch completionLatch, CountDownLatch partialLatch,
      boolean countForPartial, LiftRideEventRandomGenerator liftRideEventRandomGenerator,
      RecordWriter recordWriter) {
    this.numRequests = numRequests;
    this.counterSuccess = counterSuccess;
    this.counterFail = counterFail;
    this.completionLatch = completionLatch;
    this.partialLatch = partialLatch;
    this.countForPartial = countForPartial;
    this.liftRideEventRandomGenerator = liftRideEventRandomGenerator;

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(BASE_PATH);
    apiClient.setVerifyingSsl(false);
//    //add for concurrent connection pool

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
    boolean partialNotified = false;
    for (int i=0; i<numRequests ; i++) {
      boolean success = false;
      int retryCount = 0;
      long startTime = System.currentTimeMillis();
      while(!success && retryCount < RETIRES_THRESHOLD ) {
        try{
          LiftRideEvent liftRideEvent = liftRideEventRandomGenerator.getNextLiftRideEvent();
          LiftRide liftRide = new LiftRide();
          liftRide.setLiftID(liftRideEvent.getLiftID());
          liftRide.setTime(liftRideEvent.getTime());

          ApiResponse<Void> response = skiersApi.writeNewLiftRideWithHttpInfo(liftRide,
              liftRideEvent.getResortID(), liftRideEvent.getSeasonID(), liftRideEvent.getDayID(), liftRideEvent.getSkierID());

          long endTime = System.currentTimeMillis();
          recordWriter.addPostMetric(new PostRequestMetrics(startTime, endTime-startTime, "POST", response.getStatusCode()));

          success = true;
          completedRequest++;
          counterSuccess.incrementAndGet();

        if(countForPartial && !partialNotified && partialLatch!=null && completedRequest>=(numRequests*PHASE1_PARTIAL_THRESHOLD)){
          partialLatch.countDown();
          partialNotified = true;
        }

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
    completionLatch.countDown();
  }

}
