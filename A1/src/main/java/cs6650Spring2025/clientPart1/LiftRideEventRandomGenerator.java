package cs6650Spring2025.clientPart1;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class LiftRideEventRandomGenerator {
  private final BlockingDeque<LiftRideEvent> eventQueue;
  private final int QUEUE_CAPACITY = 10000;
  private volatile boolean running = true;
  private final String dayValue;

  public LiftRideEventRandomGenerator(String dayValue) {
    this.eventQueue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
    this.dayValue = dayValue;
  }

  public void startGeneration(){
    new Thread(() -> {
      while(running){
        try{
          eventQueue.put(new LiftRideEvent(dayValue));
        }catch(InterruptedException e){
          Thread.currentThread().interrupt();
          break;
        }
      }
    }).start();
  }

  /**
   * Take the event from the blockingqueue
   * @return
   * @throws InterruptedException
   */
  public LiftRideEvent getNextLiftRideEvent() throws InterruptedException {
    return eventQueue.take();
  }

  /**
   * Reset the stage of generation
   */
  public void stopGeneration(){
    running = false;
  }
}
