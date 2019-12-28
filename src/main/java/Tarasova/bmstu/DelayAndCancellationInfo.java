package Tarasova.bmstu;
import scala.Tuple2;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;



public class DelayAndCancellationInfo {
    private static int delimiterIndex = 0;
    private final static String DELIMITER = ",";
    private final static String AIRPORT_CSV_PATH = "L_AIRPORT_ID.csv";
    private final static String FLIGHTS_CSV_PATH = "664600583_T_ONTIME_sample.csv";

    public static void main(String[] args) {
        //Инициализация приложения
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //Загрузка данных
        JavaRDD<String> flightsCSV = sc.textFile(FLIGHTS_CSV_PATH);
        JavaRDD<String> airportsCSV = sc.textFile(AIRPORT_CSV_PATH);
        String filterFlights = flightsCSV.first();
        String filterAirport = airportsCSV.first();
        //line - <Lambbda parametr>

        flightsCSV = flightsCSV.filter(line -> !line.equals(filterFlights));
        airportsCSV = airportsCSV.filter(line -> !line.equals(filterAirport));

        //
        JavaPairRDD<Tuple2, DataOfFlight> dataOfFlight = flightsCSV.mapToPair(in -> {
            DataOfFlight flData = new DataOfFlight(in);
            return new Tuple2<>(new Tuple2<>(flData.getOriginAirportID(), flData.getDestAirportID()), flData);
        });
        //
        JavaPairRDD<Tuple2, SelectedPair> combineFlData = dataOfFlight.combineByKey(
                airport -> new SelectedPair(
                        1,
                        airport.getCancelled(),
                        airport.getDelayed(),
                        airport.getDelay()
                ),
                (pair, airport) -> pair.addFlight(
                        airport.getCancelled(),
                        airport.getDelayed(),
                        airport.getDelay()
                ),
                SelectedPair::addData
        );
        //
        Map<Integer, String> pair = airportsCSV.mapToPair(row -> {
            delimiterIndex = row.indexOf(DELIMITER);
            String airportID = unquoteAirportID(row);
            String airportName = unquoteAirportName(row);
            return new Tuple2<>(Integer.parseInt(airportID), airportName);
        }).collectAsMap();
        //
        final Broadcast<Map<Integer, String>> broadcast = sc.broadcast(pair);
        System.out.println(combineFlData.map(
                    s ->
                            "{"+ broadcast.getValue().get(s._1._1) + " | " +
                                    broadcast.getValue().get(s._1._2) + " | " +
                                    s._2.getInfoString() + "}\n").collect());
    }
    private static String unquoteAirportID(String str){
        return str.substring(1, delimiterIndex - 1);
    }
    private static String unquoteAirportName(String str){
        return str.substring(delimiterIndex + 2, str.length() - 1);
    }
}

/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import java.util.List;
import java.util.Map;

import java.util.Arrays;


public class ShowDelayFlights {
    public static void main(String[] args) {
        //инициализация приложения
        SparkConf conf = new SparkConf().setAppName("lab3").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //загрузка данных
        JavaRDD<String> flightsTable = sc.textFile("flights.csv");
        JavaRDD<String> airportsTable = sc.textFile("airports.csv");
        //Разбиение строки на слова - splits распарсить..
        JavaRDD<String[]> airports = TablesParser.splitAirportsTable(airportsTable);

        JavaRDD<String[]> flights = TablesParser.splitFlightsTable(flightsTable);
        //формируем пары <название аеропорта, его код>
        /*JavaPairRDD<String, Long> dictionary =
                dictionaryFile.mapToPair(Hadoop
                        s -> new Tuple2<>(Hadoop s,1l)
                );*/
/*
        JavaPairRDD<String, String> codeNamePairAirport = TablesParser.makeAirportPairs(airports);

        //делаем задание лабы - связываем тюпл<название, код> с (<код вылета, код прилета>, <делей, кенселед>)
        JavaPairRDD<Tuple2<String,String>, FlightKey> originDestDelayCancelledFlightTuple = TablesParser.makeFlightPair(flights); //?
        //коллект эз мап — для связывания с таблицей аэропортов — предварительно выкачиваем список
        //аэропортов в главную функцию с помощью метода collectAsMap
        // collectAsMap - Collect the result as a map to provide easy lookup
        Map<String, String> airMap = codeNamePairAirport.collectAsMap();
        //создаем в основном методе main переменную broadcast сюда кидаем пары код, имя аэропорта
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airMap);
        //c помощью функции reduce или аналогичных расчитываем максимальное
        //время опоздания, процент опоздавших+отмененных рейсов
        JavaPairRDD<Tuple2<String, String>, FlightKey> reduceData = originDestDelayCancelledFlightTuple.reduceByKey(reduceMethod.REDUCE);
        //формируем строки для результата... res должен быть:a
        // name_origin, name_dest, maxDelay, %OfLate, %OfCanceled

        JavaPairRDD<Tuple2<String, String>, List<String>> res = TablesParser.writeRes(reduceData);
        //связать вывод с именами аэропортов
        //обогащаем его именами аэропортов, обращаясь внутри
        //функций к объекту airportsBroadcasted.value()
        JavaRDD<List<String>> resFinal =
                res.map( s -> Arrays.asList(airportsBroadcasted.value().get(s._1._1),
                        airportsBroadcasted.value().get(s._1._2), String.valueOf(s._2)));

        resFinal.saveAsTextFile("flightoutput2");
    }
 }
*/