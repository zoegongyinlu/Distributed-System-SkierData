package cs6650Spring2025.clientPart1;

import io.swagger.client.model.LiftRide;
import java.util.Random;

public class LiftRideEvent {
  private final int skierID;
  private final int resortID;
  private final int liftID;
  private final String seasonID;
  private final String dayID;
  private final int time;
  private static final Random RANDOM = new Random();

  public LiftRideEvent(String dayValue) {
    this.skierID = RANDOM.nextInt(100000)+1;

//    this.resortID = RANDOM.nextInt(10) + 1;
    this.resortID = 8;
    this.liftID = RANDOM.nextInt(40) + 1;
    this.seasonID = "2025";
    this.dayID = dayValue;
    this.time = RANDOM.nextInt(360) + 1;
  }

  public LiftRideEvent(int skierID, int resortID, int liftID, String seasonID, String dayID,
      int time) {
    this.skierID = skierID;
    this.resortID = resortID;
    this.liftID = liftID;
    this.seasonID = seasonID;
    this.dayID = dayID;
    this.time = time;
  }

  public int getSkierID() {
    return skierID;
  }

  public int getResortID() {
    return resortID;
  }

  public int getLiftID() {
    return liftID;
  }

  public String getSeasonID() {
    return seasonID;
  }

  public String getDayID() {
    return dayID;
  }

  public int getTime() {
    return time;
  }


}
