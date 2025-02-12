package cs6650Spring2025.clientPart2;

public class PostRequestMetrics {
  private final long startTime;
  private final long latency;
  private final String requestType;
  private final int responseCode;

  public PostRequestMetrics(long startTime, long latency, String requestType, int responseCode) {
    this.startTime = startTime;
    this.latency = latency;
    this.requestType = requestType;
    this.responseCode = responseCode;
  }

  public String toCsvString() {
    return String.format("%d,%s,%d,%d", startTime, requestType, latency, responseCode);
  }

  public long getLatency() {return latency;}

}
