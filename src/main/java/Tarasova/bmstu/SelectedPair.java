package Tarasova.bmstu;
import java.io.Serializable;
public class SelectedPair implements Serializable {
    private double maxDelay;
    private int totalFlights;
    private int delayedFlights;
    private int cancelledFlights;
    public SelectedPair(int totalFlights, int cancelledFlights, int delayedFlights, double maxDelay){
        this.maxDelay = maxDelay;
        this.totalFlights = totalFlights;
        this.delayedFlights = delayedFlights;
        this.cancelledFlights = cancelledFlights;
    }
    public static SelectedPair addData(SelectedPair airportPair1, SelectedPair airportPair2){
        return new SelectedPair(
                airportPair1.totalFlights + airportPair2.totalFlights,
                airportPair1.cancelledFlights + airportPair2.cancelledFlights,
                airportPair1.delayedFlights + airportPair2.delayedFlights,
                Math.max(airportPair1.maxDelay, airportPair2.maxDelay)
        );
    }
    public SelectedPair addFlight(int cancelledFlight, int delayedFlight, double maxDelay){
        return new SelectedPair(
                this.totalFlights + 1,
                this.cancelledFlights + cancelledFlight,
                this.delayedFlights + delayedFlight,
                Math.max(this.maxDelay, maxDelay)
        );
    }
    public String getInfoString(){
        String infoString = "".concat("[Max delay: ").concat(Double.toString(maxDelay)).concat(", Delayed flights %: ");
        infoString = infoString.concat(Double.toString(this.getDelayedFlights())).concat(", Cancelled flights %: ");
        return infoString.concat(Double.toString(this.getCancelledFlights())).concat("]; ");
    }
    public double getMaxDelay() {return maxDelay;}
    public int getTotalFlights() {return totalFlights;}
    public int getDelayedFlights() {return delayedFlights;}
    public int getCancelledFlights() {return cancelledFlights;}
    public double getDelayPercentage(){return (double)delayedFlights / totalFlights;}
    public double getCancelledPercentage(){return (double)cancelledFlights / totalFlights;}
}