package sayservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.gdata.util.io.base.UnicodeReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.QueryBuilder;

public class AnnotatedTTGenerator {

	// input GTFS.
	private String pathToGTFS = "src/test/resources/annotatedtimetable/gtfs/google_transit_urbano/";

	private static final String UTF8_BOM = "\uFEFF";
	private static int numOfHeaders = 6;
	private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm");
	private MongoClient mongoClient = null;
	private DB database = null;
	private DBCollection collection = null;
	ObjectMapper mapper = new ObjectMapper();

	private String agencyId;
	private String routeShortName;
	private String routeId;

	private HashMap<String, List<String[]>> tripStopsTimesMap = new HashMap<String, List<String[]>>();
	private HashMap<String, List<String>> routeTripsMap = new HashMap<String, List<String>>();

	private List<String[]> routes;

	private List<String> anamolyStopIdMap = new ArrayList<String>();

	public AnnotatedTTGenerator() {
		try {
			mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
			database = mongoClient.getDB("smart-planner-15x");
			collection = database.getCollection("stops");
			mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void processFiles(String outputDir, String agency, String... files) throws Exception {
		List<String> annotated = new ArrayList<String>();
		for (String filename : files) {
			String outputName = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("."));

			File file = new File(filename);
			List<String> lines = Files.asCharSource(file, Charsets.UTF_8).readLines();
			annotated.addAll(convertLines(lines));
			File outputDirFile = new File(outputDir);
			File annotatedCSV = new File(outputDirFile, outputName + "-annotated.csv");
			Files.asCharSink(annotatedCSV, Charsets.UTF_8).writeLines(annotated);
		}

	}

	private List<String> convertLines(List<String> lines) throws Exception {

		List<String> converted = new ArrayList<String>();
		/** read as table. **/
		String[][] table = new String[lines.size()][];
		int maxNumberOfCols = 0;
		for (int i = 0; i < lines.size(); i++) {
			table[i] = StringUtils.commaDelimitedListToStringArray(lines.get(i));
			if (table[i][0].split(";").length > maxNumberOfCols) {
				/** max number of possible columns. **/
				maxNumberOfCols = table[i][0].split(";").length;
			}
		}

		/** write heading in output. **/
		for (int i = 0; i < numOfHeaders; i++) {
			converted.add(lines.get(i));
		}

		/** create local copy of table as string[][] matrix. **/
		String[][] matrix = new String[lines.size()][maxNumberOfCols + 1];
		for (int i = 0; i < lines.size(); i++) {
			String[] colValues = table[i][0].split(";");
			for (int j = 0; j < colValues.length; j++) {
				matrix[i][j] = colValues[j];
				//				System.out.println(matrix[i][j]);
			}
		}

		// extract GTFS information and structures.
		routeShortName = matrix[0][1];
		agencyId = "12"; // ?? to be put inside csv matrix[0][2].
		init(agencyId);

		// annotation process.
		int noOfOutputRows = (lines.size() * 3 / 2 - numOfHeaders + 1);
		int noOfOutputCols = maxNumberOfCols + 1;
		String[][] output = new String[noOfOutputRows][noOfOutputCols];

		output = processMatrix(matrix, output, noOfOutputCols);

		// simple print existing matrix.
		for (int i = 0; i < noOfOutputRows; i++) {
			String line = "";
			for (int j = 0; j < maxNumberOfCols; j++) {
				line = line + output[i][j] + ";";
			}
			//			System.out.println(line);
			converted.add(line);
		}

		return converted;
	}

	private String[][] processMatrix(String[][] matrix, String[][] output, int noOfOutputCols) {

		for (int i = numOfHeaders; i < matrix.length; i++) {
			for (int j = 1; j < noOfOutputCols; j++) {
				output[i - numOfHeaders + 1][j] = matrix[i][j];
			}
			//			System.out.println(Arrays.toString(output[i]));
		}

		// stops column.
		output[0][0] = "stops;stop_id";
		for (int i = numOfHeaders; i < matrix.length; i++) {
			output[i - numOfHeaders + 1][0] = processStopsColumns(matrix[i][0]);
		}

		for (int j = 1; j < noOfOutputCols - 1; j++) {
			output[0][j] = mapTOGTFS(matrix, output, numOfHeaders, j);
		}

		return output;
	}

	private String mapTOGTFS(String[][] matrix, String[][] output, int startRow, int currentCol) {

		String annotation = "";
		boolean italics = false;

		// validate trip with GTFS.
		int tripStartIndex = -1;
		for (int i = startRow; i < matrix.length; i++) {
			if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()) {
				if (matrix[i][currentCol].contains("-")) {
					italics = true;
					continue;
				}
				tripStartIndex = i;
				break;
			}
		}
		int tripEndIndex = -1;
		for (int i = matrix.length - 1; i >= startRow; i--) {
			if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()) {
				if (matrix[i][currentCol].contains("-")) {
					italics = true;
					continue;
				}
				tripEndIndex = i;
				break;
			}
		}

		String startTime = matrix[tripStartIndex][currentCol].replace(".", ":");
		String endTime = matrix[tripEndIndex][currentCol].replace(".", ":");

		routeId = getGTFSRouteIdFromRouteShortName(routeShortName);

		if (matrix[5][currentCol] != null && matrix[5][currentCol].contains("Linea")) {
			String pdfRouteId = matrix[5][currentCol].substring(matrix[5][currentCol].indexOf('a') + 1);
			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
		}

		System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
				+ startTime + "-" + endTime + "]");

		List<String> tripsForRoute = routeTripsMap.get(routeId);

		if (tripsForRoute.isEmpty()) {
			annotation = "no route found";
			return annotation;
		}

		List<String> matchingTripId = new ArrayList<String>();
		for (String tripId : tripsForRoute) {
			List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

			if (stopTimes.get(0)[1].contains(startTime) && stopTimes.get(stopTimes.size() - 1)[1].contains(endTime)) {
				//				/** first version(trip matching algorithm. **/
				//				if (!matchingTripId.contains(tripId)) {
				//					matchingTripId.add(tripId);
				//				}
				/** second version (trip matching algorithm). **/
				if (matchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
					if (!matchingTripId.contains(tripId)) {
						matchingTripId.add(tripId);
					}
					break;
				}

			}
		}

		// fill stops.
		if (matchingTripId != null && !matchingTripId.isEmpty()) {

			if (matchingTripId.size() == 1) {
				annotation = matchingTripId.get(0);
			} else {
				System.err.println("anamoly- mutliple trips detected");
				for (String tripId : matchingTripId) {
					annotation = annotation + "-" + tripId;

				}
			}

			List<String[]> stoptimeseq = tripStopsTimesMap.get(matchingTripId.get(0));
			boolean[] sequenceTraversed = new boolean[stoptimeseq.size()];
			for (int i = startRow; i < matrix.length; i++) {

				if (matrix[i][currentCol] == null || matrix[i][currentCol].isEmpty()
						|| matrix[i][currentCol].contains("|")) {
					continue;
				}
				if (matrix[i][currentCol].contains("-")) {
					italics = true;
					continue;
				}
				String timeToCheck = matrix[i][currentCol].replace(".", ":");
				for (int s = 0; s < stoptimeseq.size(); s++) {
					if (stoptimeseq.get(s)[1].contains(timeToCheck) && !sequenceTraversed[s]) {
						if (output[i - numOfHeaders + 1][0].indexOf(";") == -1) {
							output[i - numOfHeaders + 1][0] = output[i - numOfHeaders + 1][0] + ";"
									+ stoptimeseq.get(s)[3] + "_" + agencyId;
							sequenceTraversed[s] = true;
							break;
						} else {
							String stopName = output[i - numOfHeaders + 1][0].substring(0,
									output[i - numOfHeaders + 1][0].indexOf(";"));
							String stopId = "";
							if (output[i - numOfHeaders + 1][0].contains("~")) {
								stopId = output[i - numOfHeaders + 1][0].substring(output[i - numOfHeaders + 1][0]
										.indexOf(";") + 2);
							} else {
								stopId = output[i - numOfHeaders + 1][0].substring(output[i - numOfHeaders + 1][0]
										.indexOf(";") + 1);
							}
							if (!stopId.equalsIgnoreCase(stoptimeseq.get(s)[3] + "_" + agencyId)) {
								System.err.println("anamoly detected for stop id: " + "(" + stopId + ","
										+ stoptimeseq.get(s)[3] + "_" + agencyId + ")");
								if (stopId.indexOf(",") != -1) {
									String[] stops = stopId.split(",");
									for (String stp : stops) {
										if (!anamolyStopIdMap.contains(stp)) {
											anamolyStopIdMap.add(stp);
											output[i - numOfHeaders + 1][0] = stopName + ";~" + stopId + ","
													+ stoptimeseq.get(s)[3] + "_" + agencyId;
										}
									}
								} else {
									output[i - numOfHeaders + 1][0] = stopName + ";~" + stoptimeseq.get(s)[3] + "_"
											+ agencyId + "," + stopId;
									anamolyStopIdMap.add(stoptimeseq.get(s)[3] + "_" + agencyId);
									anamolyStopIdMap.add(stopId);
								}

							}
							sequenceTraversed[s] = true;
							break;
						}
					}
				}
			}
		} else {
			System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
			annotation = "no trip found";
		}

		// notes(if any).
		if (italics) {
			annotation = annotation + " * italic entry found.";
		}

		return annotation;
	}

	private boolean matchTrips(String[][] matrix, int currentCol, int tripStartIndex, int tripEndIndex,
			List<String[]> stopTimes) {

		int i = 0;
		for (i = tripStartIndex; i <= tripEndIndex; i++) {
			if (matrix[i][currentCol] == null || matrix[i][currentCol].isEmpty() || matrix[i][currentCol].contains("|")
					|| matrix[i][currentCol].contains("-")) {
				continue;

			}
			String timeToCheck = matrix[i][currentCol].replace(".", ":");
			boolean found = false;
			/** to make sure if sequence time checked once. **/
			boolean[] tripSequence = new boolean[stopTimes.size()];

			for (int s = 0; s < stopTimes.size(); s++) {
				if (stopTimes.get(s)[1].contains(timeToCheck) && !tripSequence[s]) {
					found = true;
					tripSequence[s] = true;
					break;
				}

			}
			if (!found) {
				return false;
			}
		}

		if (i == tripEndIndex + 1) {
			return true;
		}

		return false;
	}

	private String processStopsColumns(String cellValue) {
		// remove double space?
		cellValue = cellValue.replaceAll("\\s+", " ");
		String value = cellValue;
		String stopId = "";
		// query 'stops' collection for matching stopName.
		BasicDBObject regexQuery = new BasicDBObject();
		regexQuery.put("name", new BasicDBObject("$regex", ".*" + cellValue + ".*").append("$options", "-i"));
		//System.out.println(regexQuery.toString());
		DBCursor cursor = collection.find(regexQuery);
		// if arrive only one record.
		if (cursor.count() == 1) {
			BasicDBObject dbObject = (BasicDBObject) cursor.next();
			Object stop = mapper.convertValue(dbObject, Map.class);
			Map stopMap = mapper.convertValue(stop, Map.class);
			stopId = (String) stopMap.get("stopId");
			value = value + ";" + stopId;
		}

		return value;
	}

	private void init(String agencyId) throws IOException {

		String routeFile = pathToGTFS + "routes.txt";
		String tripFile = pathToGTFS + "trips.txt";
		String stopFile = pathToGTFS + "stops.txt";
		String stoptimesTFile = pathToGTFS + "stop_times.txt";
		HashMap<String, String> stopsMap = new HashMap<String, String>();

		List<String[]> linesTrip = readFileGetLines(tripFile);
		List<String[]> linesST = readFileGetLines(stoptimesTFile);
		List<String[]> stops = readFileGetLines(stopFile);
		routes = readFileGetLines(routeFile);

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
			List<String> list = routeTripsMap.get(linesTrip.get(i)[0]);
			if (list == null) {
				list = new ArrayList<String>();
				routeTripsMap.put(linesTrip.get(i)[0], list);
			}
			list.add(linesTrip.get(i)[2]);
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

	public static Object getObjectByField(DB db, String key, String value, DBCollection collection,
			Class destinationClass) {
		Object result = null;

		QueryBuilder qb = QueryBuilder.start(key).is(value);

		BasicDBObject dbObject = (BasicDBObject) collection.findOne(qb.get());

		if (dbObject != null) {
			dbObject.remove("_id");

			ObjectMapper mapper = new ObjectMapper();
			result = mapper.convertValue(dbObject, destinationClass);
		}

		return result;
	}

	private String formatTime(String string) throws ParseException {
		return TIME_FORMAT.format(TIME_FORMAT.parse(string));
	}

	private String getGTFSRouteIdFromRouteShortName(String routeShortName) {
		String routeId = "";
		for (String[] words : routes) {
			try {

				if (words[2].equalsIgnoreCase(routeShortName.trim())) {
					routeId = words[0];
					break;
				}

			} catch (Exception e) {
				System.out.println("Error parsing route: " + words[0] + "," + words[1] + "," + words[2]);
			}
		}
		return routeId;
	}

	public static void main(String[] args) throws Exception {
		AnnotatedTTGenerator timeTableGenerator = new AnnotatedTTGenerator();

		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable", "12",
				"src/test/resources/annotatedtimetable/05A-Feriale.csv");
		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable", "12",
				"src/test/resources/annotatedtimetable/05A-Festivo.csv");
		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable", "12",
				"src/test/resources/annotatedtimetable/05R-Feriale.csv");
		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable", "12",
				"src/test/resources/annotatedtimetable/05R-Festivo.csv");

	}
}
