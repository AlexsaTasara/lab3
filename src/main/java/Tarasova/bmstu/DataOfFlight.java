package Tarasova.bmstu;
import java.io.Serializable;

public class DataOfFlight implements Serializable {
    private double delay;
    private int destAirportID;
    private int originAirportID;
    private int delayed, cancelled;
    private static final int CANCELLED_INDEX = 19;
    private static final int FLIGHT_DELAY_INDEX = 17;
    private static final int DEST_AIRPORT_ID_INDEX = 14;
    private static final int ORIGIN_AIRPORT_ID_INDEX = 11;

    public DataOfFlight(String flData) throws NumberFormatException {
        String[] table = flData.split(",");
        this.cancelled = (int) (Double.parseDouble(table[CANCELLED_INDEX]));
        this.destAirportID = Integer.parseInt(table[DEST_AIRPORT_ID_INDEX]);
        this.originAirportID = Integer.parseInt(table[ORIGIN_AIRPORT_ID_INDEX]);

        if (cancelled == 0 && table[FLIGHT_DELAY_INDEX].length() != 0) {
            delay = Double.parseDouble(table[FLIGHT_DELAY_INDEX]);
        } else {
            this.delay = 0;
        }
        if (delay > 0.0) {
            this.delayed = 1;
        } else {
            this.delayed = 0;
        }
    }

    public double getDelay() {return delay;}
    public int getDelayed() {return delayed;}
    public int getCancelled() {return cancelled;}
    public int getDestAirportID() {return destAirportID;}
    public int getOriginAirportID() {return originAirportID;}
}