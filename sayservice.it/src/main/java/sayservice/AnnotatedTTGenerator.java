package sayservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.bson.BasicBSONObject;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gdata.util.io.base.UnicodeReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.QueryBuilder;
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.NoFixedFacet;

public class AnnotatedTTGenerator {

	// input GTFS.
	private String pathToGTFS = "src/test/resources/annotatedtimetable/gtfs/google_transit_urbano/";

	private static final String UTF8_BOM = "\uFEFF";
	private static final String ITALIC_ENTRY = "italic";
	private static final String ROUTE_ERROR = "route not found";
	private static final String TRIP_ERROR = "trip not found";
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
	private HashMap<String, String> stopsMap = new HashMap<String, String>();
	private HashMap<Integer, List<String>> columnTripIdMap = new HashMap<Integer, List<String>>();
	private HashMap<Integer, List<String>> columnHeaderNotes = new HashMap<Integer, List<String>>();
	private HashMap<Integer, List<String>> columnItalicStopNames = new HashMap<Integer, List<String>>();
	private HashMap<String, String> stopIdsMap = new HashMap<String, String>();

	private List<String[]> routes;

	private List<String> anamolyStopIdMap = new ArrayList<String>();

	private List<String> anomalyStopIds = new ArrayList<String>();

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
			
			destroy();
		}

	}

	private void destroy() {
		anamolyStopIdMap.clear();
		anomalyStopIds.clear();
		columnHeaderNotes.clear();
		columnItalicStopNames.clear();
		columnTripIdMap.clear();
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
			converted.add(lines.get(i).replaceFirst(";", ";;"));
		}

		/** create local copy of table as string[][] matrix. **/
		String[][] matrix = new String[lines.size()][maxNumberOfCols + 1];
		for (int i = 0; i < lines.size(); i++) {
			String[] colValues = table[i][0].split(";");
			for (int j = 0; j < colValues.length; j++) {
				matrix[i][j] = colValues[j];
				// System.out.println(matrix[i][j]);
			}
		}

		// extract GTFS information and structures.
		routeShortName = matrix[0][1];
		agencyId = "12"; // ?? to be put inside csv matrix[0][2].
		init(agencyId);

		// annotation process.
		int noOfOutputRows = (lines.size() * 2 - numOfHeaders + 1);
		int noOfOutputCols = maxNumberOfCols + 1;
		String[][] output = new String[noOfOutputRows][noOfOutputCols];

		output = processMatrix(matrix, output, noOfOutputCols);

		// simple print existing matrix.
		for (int i = 0; i < noOfOutputRows; i++) {
			String line = "";
			for (int j = 0; j < maxNumberOfCols; j++) {
				line = line + output[i][j] + ";";
			}
			// System.out.println(line);
			converted.add(line);
		}

		return converted;
	}

	private String[][] processMatrix(String[][] matrix, String[][] output, int noOfOutputCols) {

		/** version 1.
		for (int i = numOfHeaders; i < matrix.length; i++) {
			for (int j = 1; j < noOfOutputCols; j++) {
				output[i - numOfHeaders + 1][j] = matrix[i][j];
			}
			//	System.out.println(Arrays.toString(output[i]));
		}**/

		// stops column.
		output[0][0] = "stops;stop_id";
		// create list of stops taking in to consideration GTFS data.
		List<String> stops = processStops(matrix, numOfHeaders, noOfOutputCols - 1);

		for (int j = 1; j < noOfOutputCols - 1; j++) {
			if (columnTripIdMap.containsKey(j)) {
				List<String[]> stoptimeseq = tripStopsTimesMap.get(columnTripIdMap.get(j).get(0));
				for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

					String time = stoptimeseq.get(gtfsSeq)[1];
					String id = stoptimeseq.get(gtfsSeq)[3];
					String stopListName = stopsMap.get(id).toLowerCase();
					output[stops.indexOf(stopListName) + 1][j] = stoptimeseq.get(gtfsSeq)[1].substring(0,
							time.lastIndexOf(":"));

				}

			}

			// fill in italic entries.
			for (String italicEntry : columnItalicStopNames.get(j)) {
				String name = italicEntry.substring(0, italicEntry.indexOf("$"));
				String time = italicEntry.substring(italicEntry.indexOf("$") + 1);
				output[stops.indexOf(name) + 1][j] = time;
			}
		}

		/** version 1.
		for (int i = numOfHeaders; i < matrix.length; i++) {
			output[i - numOfHeaders + 1][0] = processStopsColumns(matrix[i][0]);
		}

		for (int j = 1; j < noOfOutputCols - 1; j++) {
			output[0][j] = mapTOGTFS(matrix, output, numOfHeaders, j);
		}**/

		for (int i = 0; i < stops.size(); i++) {

			if (stopIdsMap.containsKey(stops.get(i))) {
				String stopId = stopIdsMap.get(stops.get(i));
				if (stopsMap.containsKey(stopId)) {
					output[i + 1][0] = stopsMap.get(stopId) + ";" + stopId;
					if (anomalyStopIds.contains(stopId)) {
						output[i + 1][0] = "*" + output[i + 1][0];
					}
				} else {
					output[i + 1][0] = stops.get(i) + ";" + stopId;
					if (anomalyStopIds.contains(stopId)) {
						output[i + 1][0] = "*" + output[i + 1][0];
					}
				}

			} else {
				output[i + 1][0] = stops.get(i) + ";";
			}
		}

		for (int col = 1; col < noOfOutputCols - 1; col++) {
			output[0][col] = fillHeaderAnnotation(stops, col);
		}

		output = clean(output);

		return output;
	}

	public static String[][] clean(String[][] array) {
		for (int i = 0; i < array.length; i++) {
			String[] inner = array[i];
			for (int j = 0; j < inner.length - 1; j++) {
				if (inner[j] == null) {
					inner[j] = "";
				}
			}
		}
		return array;
	}

	private String fillHeaderAnnotation(List<String> stops, int col) {

		List<String> tripIds = columnTripIdMap.get(col);
		String annotation = "";

		if (tripIds != null) {
			if (tripIds.size() == 1) {
				// exact
				annotation = tripIds.get(0);

			} else if (tripIds.size() > 1) {
				// multiple trips.
				for (String tripId : tripIds) {
					annotation = annotation + tripId + ",";
				}

			}
		}

		// additional notes.
		for (String note : columnHeaderNotes.get(col)) {
			annotation = annotation + note;
		}

		// TODO Auto-generated method stub
		return annotation;
	}

	private List<String> processStops(String[][] matrix, int startRow, int noOfCols) {

		// merged list of stops.
		List<String> stopList = new ArrayList<String>();
		// pdf list of stops.
		List<String> pdfStopList = new ArrayList<String>();
		List<Integer> anamolies = null;

		for (int i = 0; i < (matrix.length - numOfHeaders); i++) {
			pdfStopList.add(matrix[i + numOfHeaders][0]);
		}

		// add all pdf stop first to final list.
		stopList.addAll(pdfStopList);

		Map<String, List<Integer>> anamolyMap = new HashMap<String, List<Integer>>();

		for (int currentCol = 1; currentCol < noOfCols; currentCol++) {

			boolean italics = false;
			boolean mergedRoute = false;

			// additional notes for column map.
			List<String> columnNotes = new ArrayList<String>();
			columnHeaderNotes.put(currentCol, columnNotes);

			// column italic stopNames.
			List<String> italicStopEntry = new ArrayList<String>();
			columnItalicStopNames.put(currentCol, italicStopEntry);

			int tripStartIndex = -1;
			for (int i = startRow; i < matrix.length; i++) {
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()) {
					if (matrix[i][currentCol].contains("-")) {
						italics = true;
						columnNotes.add(ITALIC_ENTRY);
						String stopName = matrix[i][0].replaceAll("\\s+", " ").toLowerCase();
						String time = matrix[i][currentCol];
						if (!italicStopEntry.contains(stopName + "$" + time)) {
							italicStopEntry.add(stopName + "$" + time);
						}
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
						if (!columnNotes.contains(ITALIC_ENTRY)) {
							columnNotes.add(ITALIC_ENTRY);
						}
						String stopName = matrix[i][0].replaceAll("\\s+", " ").toLowerCase();
						String time = matrix[i][currentCol];
						if (!italicStopEntry.contains(stopName + "$" + time)) {
							italicStopEntry.add(stopName + "$" + time);
						}
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
				mergedRoute = true;
			} else if (matrix[5][currentCol] != null && isInteger(matrix[5][currentCol])) {
				String pdfRouteId = matrix[5][currentCol];
				routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
				mergedRoute = true;
			}

			System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
					+ startTime + "-" + endTime + "]");

			if (routeId != null && !routeId.isEmpty()) {

				List<String> tripsForRoute = routeTripsMap.get(routeId);

				if (tripsForRoute.isEmpty()) {
					System.err.println("no route found");
					columnNotes.add(ROUTE_ERROR);
				}

				List<String> matchingTripId = new ArrayList<String>();
				for (String tripId : tripsForRoute) {
					List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

					if (stopTimes.get(0)[1].contains(startTime)
							&& stopTimes.get(stopTimes.size() - 1)[1].contains(endTime)) {

						if (mergedRoute) {
							/** first version(trip matching algorithm. **/
							if (!matchingTripId.contains(tripId)) {
								matchingTripId.add(tripId);
								break;
							}
						} else {
							/** second version (trip matching algorithm). **/
							if (matchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
								if (!matchingTripId.contains(tripId)) {
									matchingTripId.add(tripId);
								}
								break;
							}
						}

					}
				}

				// prepare stops list.
				if (matchingTripId != null && !matchingTripId.isEmpty()) {

					columnTripIdMap.put(currentCol, matchingTripId);

					List<String[]> stoptimeseq = tripStopsTimesMap.get(matchingTripId.get(0));
					for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

						boolean found = false;
						String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "")
								.toLowerCase();

						for (int i = 0; i < pdfStopList.size(); i++) {
							// pdf sequence = i + numOfHeaders;
							String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
							pdfStopName = pdfStopName.replaceAll("\"", "");
							String pdfTime = "";
							if (matrix[i + numOfHeaders][currentCol] != null
									&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {
								pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";
							}
							stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(),
									stoptimeseq.get(gtfsSeq)[3]);
							if (pdfStopName.equalsIgnoreCase(gtfsStopName)
									&& stoptimeseq.get(gtfsSeq)[1].equalsIgnoreCase(pdfTime)
									&& stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
								stopList.set(i, stopsMap.get(stoptimeseq.get(gtfsSeq)[3]));
								found = true;
								// System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
								break;
							}
						}

						if (!found && stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
							anamolies = anamolyMap.get(matchingTripId.get(0));
							if (anamolies == null) {
								anamolies = new ArrayList<Integer>();
								anamolyMap.put(matchingTripId.get(0), anamolies);
							}
							anamolies.add(gtfsSeq);
							//	System.err.println( "anamoly - " +  stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
						}
					}

				} else {
					System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
					columnNotes.add(TRIP_ERROR);

				}

			}

		}

		// adding anamolies.
		for (String tripId : anamolyMap.keySet()) {

			List<Integer> anamoliesList = anamolyMap.get(tripId);
			List<String[]> stoptimeseq = tripStopsTimesMap.get(tripId);

			for (int anamoly : anamoliesList) {

				String stopNameBefore = null;
				for (int a = anamoly - 1; a > -1; a--) {
					stopNameBefore = stopsMap.get(stoptimeseq.get(a)[3]);
					if (stopNameBefore != null && !stopNameBefore.isEmpty()) {
						break;
					}
				}
				// add anomaly stop in correct position.
				if (stopList.indexOf(stopNameBefore) != -1) {
					int insertIndex = stopList.indexOf(stopNameBefore) + 1;
					String stopName = stopsMap.get(stoptimeseq.get(anamoly)[3]);
					if (stopList.indexOf(stopName) == -1) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]));
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						anomalyStopIds .add(stoptimeseq.get(anamoly)[3]);
					}
				}
			}

		}

		List<String> stopsFinal = new ArrayList<String>();

		// remove duplicate stops.
		for (String stop : stopList) {
			if (stopIdsMap.containsKey(stop)) {
				stopsFinal.add(stop.toLowerCase());
			} else {
				String pdfStopName = stop.replaceAll("\\s+", " ");
				System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
				if (!stopsFinal.contains(stop.toLowerCase()))
					stopsFinal.add(pdfStopName.toLowerCase());
			}
		}

		//		return stopList;
		return stopsFinal;
	}

	private String mapTOGTFS(String[][] matrix, String[][] output, int startRow, int currentCol) {

		String annotation = "";
		boolean italics = false;
		boolean mergedRoute = false;

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
			mergedRoute = true;
		} else if (matrix[5][currentCol] != null && isInteger(matrix[5][currentCol])) {
			String pdfRouteId = matrix[5][currentCol];
			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
			mergedRoute = true;
		}

		System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
				+ startTime + "-" + endTime + "]");

		if (routeId != null && !routeId.isEmpty()) {

			List<String> tripsForRoute = routeTripsMap.get(routeId);

			if (tripsForRoute.isEmpty()) {
				annotation = "no route found";
				return annotation;
			}

			List<String> matchingTripId = new ArrayList<String>();
			for (String tripId : tripsForRoute) {
				List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

				if (stopTimes.get(0)[1].contains(startTime) && stopTimes.get(stopTimes.size() - 1)[1].contains(endTime)) {

					if (mergedRoute) {
						/** first version(trip matching algorithm. **/
						if (!matchingTripId.contains(tripId)) {
							matchingTripId.add(tripId);
							break;
						}
					} else {
						/** second version (trip matching algorithm). **/
						if (matchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
							if (!matchingTripId.contains(tripId)) {
								matchingTripId.add(tripId);
							}
							break;
						}
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
								String stopName = output[i - numOfHeaders + 1][0].substring(0, output[i - numOfHeaders
										+ 1][0].indexOf(";"));
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
		} else {
			System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][currentCol]);
			annotation = "no route found";
		}

		// notes(if any).
		if (italics) {
			annotation = annotation + " * italic entry found.";
		}

		return annotation;
	}

	/**
	 * Utility method for checking integer
	 * @param s
	 * @return
	 */
	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
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
		//		HashMap<String, String> stopsMap = new HashMap<String, String>();

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
        File folder = new File("src/test/resources/annotatedtimetable");
		
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory() | fileEntry.getName().contains(".json")) {
	            continue;
	        } else {
	            System.out.println("Annotation in process for ->  " + fileEntry.getName());
	            timeTableGenerator.processFiles("src/test/resources/annotatedtimetable/output", "12",
	    				"src/test/resources/annotatedtimetable/" + fileEntry.getName());
	        }
	    }
		
//		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable/output", "12",
//				"src/test/resources/annotatedtimetable/03A-Feriale.csv");
//		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable/output", "12",
//				"src/test/resources/annotatedtimetable/05A-Festivo.csv");
//		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable/output", "12",
//				"src/test/resources/annotatedtimetable/05R-Feriale.csv");
//		timeTableGenerator.processFiles("src/test/resources/annotatedtimetable/output", "12",
//				"src/test/resources/annotatedtimetable/05R-Festivo.csv");

	}
	
}
