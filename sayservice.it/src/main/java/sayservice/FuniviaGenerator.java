package sayservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.Iterables;
import com.google.gdata.util.io.base.UnicodeReader;

public class FuniviaGenerator {

	private static final String UTF8_BOM = "\uFEFF";
	private static String routeId = "531";
	private static List<String[]> routes;
	private static String pathToGTFS = "src/test/resources/gtfs/12/";//"C:/tmp/otp/ftp/25.06.17/google_transit_urbano/";

	private static HashMap<String, List<String[]>> tripStopsTimesMap = new HashMap<String, List<String[]>>();
	private static HashMap<String, List<String>> routeTripsMap = new HashMap<String, List<String>>();
	private static Map<String, String> tripServiceIdMap = new HashMap<String, String>();
	private static HashMap<String, String> stopsMap = new HashMap<String, String>();
	private static List<String> agencyRoutesList = new ArrayList<String>();
	// direction.	
//	private static String directionIdentifier = "Funivia-Staz. di Monte-Sardagna"; //ANDATA.
//	private static String smartplannerRouteID = "FUTSA";
	private static String directionIdentifier = "Funivia-Staz. di Valle-Trento"; //RITORNO.
	private static String smartplannerRouteID = "FUTSR";
	// service id.
	private static String serviceId = "0000001402017061020170910";

	public static void main(String args[]) {
		try {
			init();
			// get all trips for route.
			List<String> trips = routeTripsMap.get(routeId);
			String rowGTFSTripId = "gtfs trip_id;;";
			String rowSPRouteID = "smartplanner route_id;;";
			String rowServiceId = "service_id;;";
			String rowStops = "stops;stop_id;";
			// ANDATA.
//			String rowStopStartName = "Funivia-Staz. di Valle-Trento;2939;";
//			String rowStopEndName = "Funivia-Staz. di Monte-Sardagna;2940;";
			// RITORNO.
			String rowStopStartName = "Funivia-Staz. di Monte-Sardagna;2940;";
			String rowStopEndName = "Funivia-Staz. di Valle-Trento;2939;";
			int numTrips = 0;
			// read trip one by and write csv based on stopTimes.txt(stops, stopId, tripId).
			for (String tripIdElements : trips) {

				String[] tripString = tripIdElements.split(",");
				String tripId = tripString[0];

				if (tripString[1].equalsIgnoreCase(directionIdentifier)
						&& tripServiceIdMap.get(tripId).equalsIgnoreCase(serviceId)) {

					numTrips++;

					rowGTFSTripId = rowGTFSTripId + tripId + ";";
					rowSPRouteID = rowSPRouteID + smartplannerRouteID + ";";
					rowServiceId = rowServiceId + tripServiceIdMap.get(tripId) + ";";
					rowStops = rowStops + tripId + ";";

					List<String[]> stopTimes = tripStopsTimesMap.get(tripId);
					rowStopStartName = rowStopStartName
							+ stopTimes.get(0)[2].substring(0, stopTimes.get(0)[2].lastIndexOf(":")) + ";";
					rowStopEndName = rowStopEndName
							+ stopTimes.get(1)[2].substring(0, stopTimes.get(1)[2].lastIndexOf(":")) + ";";
				}

			}

			System.out.println(rowGTFSTripId);
			System.out.println(rowSPRouteID);
			System.out.println(rowServiceId);
			System.out.println(rowStops);
			System.out.println(rowStopStartName);
			System.out.println(rowStopEndName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public FuniviaGenerator() throws IOException {
		init();
	}

	private static void init() throws IOException {

		String routeFile = pathToGTFS + "routes.txt";
		String tripFile = pathToGTFS + "trips.txt";
		String stopFile = pathToGTFS + "stops.txt";
		String stoptimesTFile = pathToGTFS + "stop_times.txt";

		List<String[]> linesTrip = readFileGetLines(tripFile);
		List<String[]> linesST = readFileGetLines(stoptimesTFile);
		List<String[]> stops = readFileGetLines(stopFile);
		routes = readFileGetLines(routeFile);

		for (String[] words : routes) {
			if (!agencyRoutesList.contains(words[0]) & !(words[0].equalsIgnoreCase("route_id"))) {
				agencyRoutesList.add(words[0]);
			}
		}

		for (int i = 0; i < stops.size(); i++) {
			String stopId = stops.get(i)[0];
			if (!stopsMap.containsKey(stops.get(i)[0])) {
				stopsMap.put(stopId, stops.get(i)[2]);
			}

		}

		for (int i = 0; i < linesST.size(); i++) {
			List<String[]> list = tripStopsTimesMap.get(linesST.get(i)[0]);
			if (list == null) {
				list = new ArrayList<String[]>();
				tripStopsTimesMap.put(linesST.get(i)[0], list);
			}
			list.add(linesST.get(i));
		}

		for (int i = 0; i < linesTrip.size(); i++) {
			tripServiceIdMap.put(linesTrip.get(i)[2], linesTrip.get(i)[1]);
		}

		for (int i = 0; i < linesTrip.size(); i++) {
			if (agencyRoutesList.contains(linesTrip.get(i)[0])) {
				List<String> list = routeTripsMap.get(linesTrip.get(i)[0]);
				if (list == null) {
					list = new ArrayList<String>();
					routeTripsMap.put(linesTrip.get(i)[0], list);
				}
				list.add(linesTrip.get(i)[2] + "," + linesTrip.get(i)[3]);
			}
		}

	}

	private static List<String[]> readFileGetLines(String fileName) throws IOException {
		FileInputStream fis = new FileInputStream(new File(fileName));
		UnicodeReader ur = new UnicodeReader(fis, "UTF-8");

		List<String[]> lines = new ArrayList<String[]>();
		for (CSVRecord record : CSVFormat.DEFAULT.parse(ur)) {
			String[] line = Iterables.toArray(record, String.class);
			lines.add(line);
		}
		lines.get(0)[0] = lines.get(0)[0].replaceAll(UTF8_BOM, "");

		return lines;
	}

}
