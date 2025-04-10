package cs6650Spring2025.clientPart2;

public class PostRequestMetrics {
  private final long startTime;
  private final long latency;
  private final String requestType;
  private final int responseCode;
  private final int skierID;
  private final String dayID;

  public PostRequestMetrics(long startTime, long latency, String requestType, int responseCode, int skierID, String dayID) {
    this.startTime = startTime;
    this.latency = latency;
    this.requestType = requestType;
    this.responseCode = responseCode;
    this.skierID = skierID;
    this.dayID = dayID;

  }

  public String toCsvString() {
    return String.format("%d,%s,%d,%d, %d, %s", startTime, requestType, latency, responseCode, skierID, dayID);
  }

  public long getLatency() {return latency;}

}
