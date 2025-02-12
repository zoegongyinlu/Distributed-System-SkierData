package cs6650Spring2025.clientPart2;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class RecordWriter {
  private static final double P_99 = 0.99;
  private final String fileName;
  private final List<PostRequestMetrics> postMetrics = Collections.synchronizedList(new ArrayList<PostRequestMetrics>());

  public RecordWriter(String fileName) {
    this.fileName = fileName;
    File file = new File(fileName);
    try(FileWriter fileWriter = new FileWriter(fileName)){

      fileWriter.write("startTime, requestType, latency, responseCode\n");

    }catch (Exception e){
      e.printStackTrace();
    }
  }

  public void addPostMetric(PostRequestMetrics postMetric) {
    this.postMetrics.add(postMetric);

    try(FileWriter fileWriter = new FileWriter(fileName, true)){
      fileWriter.write(postMetric.toCsvString() + "\n");

    }catch (Exception e){
      e.printStackTrace();
    }
  }

  /**
   * Get the post metrics copy
   * @return
   */
  public List<PostRequestMetrics> getPostMetrics() {return new ArrayList<>(postMetrics);}

  public void calculateStatistics() {
    Stream<PostRequestMetrics> metricStream = postMetrics.stream();
    LongStream latencyStream = metricStream.mapToLong(PostRequestMetrics::getLatency);
    long[] sortedLatencies = latencyStream.sorted().toArray();
    int latencyLen = sortedLatencies.length;
    double mean = Arrays.stream(sortedLatencies).average().getAsDouble();

    double median = latencyLen%2==0? (double) (sortedLatencies[latencyLen/2-1]+sortedLatencies[latencyLen/2])/2.0 : sortedLatencies[latencyLen/2] ;
    int p99Index = (int) Math.ceil(P_99*latencyLen);
    long p99Latency = sortedLatencies[p99Index-1];

    long min = sortedLatencies[0];
    long max = sortedLatencies[latencyLen-1];

    System.out.println("\nRequest Statistics:");
    System.out.println("Mean response time: " + String.format("%.2f", mean) + " ms");
    System.out.println("Median response time: " + String.format("%.2f", median) + " ms");
    System.out.println("p99 response time: " + p99Latency + " ms");
    System.out.println("Min response time: " + min + " ms");
    System.out.println("Max response time: " + max + " ms");


  }

}
