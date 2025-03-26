### `LiftRideMessageConsumer` class

The `LiftRideMessageConsumer` class is responsible for consuming messages that are published to the message queues. I increased the number of message queues to consume the messages published to the broker. 


### `PostRequestSingleThread` Class

The `PostRequestSingleThread` class is responsible for sending HTTP POST requests to the Skiers API. I changed
the ``BASE_PATH`` between localhost8080 portal and EC2 instance public IP portal.
#### Configuration

The BASE_PATH in PostRequestSingleThread should be updated based on the desired target:

```java
private static final String BASE_PATH = "http://44.233.246.8:8080/A1_war/"; // EC2 instance
 private static final String BASE_PATH = "http://servlet-ELB-422677375.us-west-2.elb.amazonaws.com/A1_war"; //ELB
private static final String BASE_PATH = "http://localhost:8080/A1_war_exploded"; // Localhost
```

Uncomment the appropriate line depending on whether requests should be directed to an EC2 instance or a local server.

### `SkierClientMain` class
The `SkierClientMain` class manages the execution of multiple threads that send HTTP POST requests. It:

- Defines the total number of requests and threads used.

- Tracks request success and failure counts.

- Calculates and prints performance metrics, 

- Saves request performance statistics using RecordWriter.
  Running the Program

### Compile and run SkierClientMain.

Monitor the console output for request statistics and execution performance.

Check the generated PostRequestMetrics.csv file for detailed request metrics.



### EC2- Instance Setup (All should automatically run, but manual setup is provided)
1. servlet (Tomcat): 
```bash
sudo systemctl start tomcat.service 
# Or mannually start
sudo /usr/share/apache-tomcat-9.0.93/bin/startup.sh
```

2. java message Consumer instance: 
```bash
java -jar /opt/Assignment/skier-app/messageConsumer/A1-1.0-SNAPSHOT.jar
```