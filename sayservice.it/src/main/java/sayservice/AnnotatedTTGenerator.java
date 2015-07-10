package sayservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.google.gdata.util.common.html.HtmlToText;
import com.google.gdata.util.io.base.UnicodeReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.QueryBuilder;

public class AnnotatedTTGenerator {

	// statistics details.
	private static boolean stats = true;
	// deep search mode.
	private static boolean deepMode = false;
	// route stats.
	private static boolean routeStats = false;
	// overall stats.
	private static boolean gtfsStats = false;
	// csv stats.
	private static boolean csvStats = true;

	// input GTFS.
//	private static final String pathToGTFS = "src/test/resources/gtfs/12/";
//	private static final String pathToGTFS = "src/test/resources/gtfs/16/";
	private static final String pathToGTFS = "src/test/resources/gtfs/17/";
	// output folder.
//	private static final String pathToOutput = "src/test/resources/annotatedtimetable/12/";
//	private static final String pathToOutput = "src/test/resources/annotatedtimetable/16/";
	private static final String pathToOutput = "src/test/resources/annotatedtimetable/17/";
	// input folder.
//	private static final String pathToInput = "src/test/resources/inputtimetable/12/";
//	private static final String pathToInput = "src/test/resources/inputtimetable/16/";
	private static final String pathToInput = "src/test/resources/inputtimetable/17/";
	// agencyIds (12,16,17)
	private static final String agencyId = "17";
	private static final List<String> roveretoNBuses = Arrays.asList("N1", "N2", "N3", "N5", "N6");
	private static final List<String> exUrbTrenoRoutes = Arrays.asList("578", "518", "352");
	private static final Map<String, String> unalignedRoutesMap = new HashMap<String, String>();
	{
		unalignedRoutesMap.put("119", "109");
	}
	private static final String exUrbanArrivalSymbol = " - arr.";
	private static final String exUrbanDepartureSymbol = " - part.";
	private static final String UTF8_BOM = "\uFEFF";
	private static final String ITALIC_ENTRY = "italic";
	private static final String ROUTE_ERROR = "route not found";
	private static final String TRIP_ERROR = "trip not found";
	private static final String GTFS_RS_NAME = "GTFS_RS_Name";
	private static int numOfHeaders = 6;
	private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm");

	private MongoClient mongoClient = null;
	private DB database = null;
	private DBCollection collection = null;
	ObjectMapper mapper = new ObjectMapper();
	DecimalFormat formatter = new DecimalFormat("00");
	
	private String routeShortName;
	private String routeId;

	private HashMap<String, List<String[]>> tripStopsTimesMap = new HashMap<String, List<String[]>>();
	private HashMap<String, List<String>> routeTripsMap = new HashMap<String, List<String>>();
	private HashMap<String, String> stopsMap = new HashMap<String, String>();
	private HashMap<Integer, List<String>> columnTripIdMap = new HashMap<Integer, List<String>>();
	private HashMap<Integer, List<String>> columnHeaderNotes = new HashMap<Integer, List<String>>();
	private HashMap<Integer, List<String>> columnItalicStopNames = new HashMap<Integer, List<String>>();
	private HashMap<String, String> stopIdsMap = new HashMap<String, String>();
	private HashMap<Integer, String> columnGTFSRSName = new HashMap<Integer, String>();

	private List<String[]> routes;

	private List<String> anamolyStopIdMap = new ArrayList<String>();

	private List<String> anomalyStopIds = new ArrayList<String>();

	// stats variables.
	private static double failedMatch = 0;
	private static double successMatch = 0;
	private static Map<String, String> fileColumnMismatchMap = new HashMap<String, String>();
	private static List<String> agencyRoutesList = new ArrayList<String>();
	private static List<String> matchedTripIds = new ArrayList<String>();
	private static List<String> deepMatchedTripIds = new ArrayList<String>();
	private static List<String> gtfsTripIds = new ArrayList<String>();
	private int totalCSVTrips;
	
	String mismatchColIds = "";
	
	

	public AnnotatedTTGenerator() throws IOException {
		try {
			mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
			database = mongoClient.getDB("smart-planner-15x");
			collection = database.getCollection("stops");
			mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			init(agencyId);

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
			if (!outputDirFile.exists()) {
				outputDirFile.mkdir();
			}
			File annotatedCSV = new File(outputDirFile, outputName + "-annotated.csv");
			Files.asCharSink(annotatedCSV, Charsets.UTF_8).writeLines(annotated);

			fileColumnMismatchMap.put(outputName + "-annotated.csv", mismatchColIds);
			destroy();
		}

	}

	private void destroy() {

		anamolyStopIdMap.clear();
		anomalyStopIds.clear();
		columnHeaderNotes.clear();
		columnItalicStopNames.clear();
		columnTripIdMap.clear();
		columnGTFSRSName.clear();
		mismatchColIds = "";

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
			String tableString = "";
			if (table[i].length > 1) {
				for (int j = 0; j < table[i].length; j++) {
					tableString = tableString + table[i][j];
				}
			} else {
				tableString = table[i][0];
			}

			//String[] colValues = table[i][0].split(";");
			String[] colValues = tableString.split(";");
			for (int j = 0; j < colValues.length; j++) {
				matrix[i][j] = colValues[j];
				// System.out.println(matrix[i][j]);
			}
		}

		// extract GTFS information and structures.
		routeShortName = matrix[0][1].replaceAll("\"","");
//		agencyId = "12"; // ?? to be put inside csv matrix[0][2].
//		init(agencyId);

		// annotation process.
		int noOfOutputCols = maxNumberOfCols + 1;
		
		String[][] output;
//		if (agencyId.equalsIgnoreCase("17")) {
//			output = processExUrbanMatrix(matrix, noOfOutputCols);
//		} else {
//			output = processExUrbanMatrix(matrix, noOfOutputCols);	
//		}
		output = processMatrix(matrix, noOfOutputCols);

		// simple print existing matrix.
		for (int i = 0; i < output.length; i++) {
			String line = "";
			for (int j = 0; j < maxNumberOfCols; j++) {
				line = line + output[i][j] + ";";
			}
			// System.out.println(line);
			converted.add(line);
		}

		return converted;
	}

	private String[][] processMatrix(String[][] matrix, int noOfOutputCols) {

		// create list of stops taking in to consideration GTFS data.
		List<String> stops = new ArrayList<String>();
		
		// prepare list of List<String> for input matrix.
		List<List<String>> inputCSVTimes = createListOfList(matrix, numOfHeaders, noOfOutputCols);
		
		stops = processStops(matrix, numOfHeaders, noOfOutputCols - 1, inputCSVTimes);

		int noOfOutputRows = (stops.size() + 1);
		String[][] output = new String[noOfOutputRows][noOfOutputCols];
		
		for (int j = 1; j < noOfOutputCols - 1; j++) {
			if (columnTripIdMap.containsKey(j)) {
				List<String[]> stoptimeseq = tripStopsTimesMap.get(columnTripIdMap.get(j).get(0));
				boolean traversed[] = new boolean[stops.size()];
				for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

					boolean isAnomalyStop = false;
					String gtfsTime = stoptimeseq.get(gtfsSeq)[2];
					String gtfs2pdfMappedTime = gtfsTime.substring(0, gtfsTime.lastIndexOf(":")).trim();
					
					String id = stoptimeseq.get(gtfsSeq)[3];
					String stopListName = stopsMap.get(id).toLowerCase();
					
					
					String anomalyKey = id + "_" + gtfs2pdfMappedTime; 
					if (anomalyStopIds.contains(anomalyKey)) {
						isAnomalyStop = true;	
					}
					
					
					/** logic for handling cyclic trips for e.g. A_C festivo.**/
					int foundIndex = -1;
					for (int i = 0; i < stops.size(); i++) {
						
						// logic for taking stopName of final List.
						String stopName = stops.get(i); 
						if (stops.get(i).contains(exUrbanDepartureSymbol)) {
							String[] stopMatchPart = stops.get(i).split(exUrbanDepartureSymbol);
							stopName = stopMatchPart[0]; 
						} else if (stops.get(i).contains(exUrbanArrivalSymbol)) {
							String[] stopMatchPart = stops.get(i).split(exUrbanArrivalSymbol);
							stopName = stopMatchPart[0];
						}
						
						if (isAnomalyStop) {
							if (stopName.equals(stopListName) && !traversed[i]) {
								foundIndex = i;
								break;
							}
							
						} else {
							List<String> inputCsvTimeList = null;
							if (inputCSVTimes.get(i) != null) {
								inputCsvTimeList = inputCSVTimes.get(i);
							}
							
							String inputPdfTime = "";
							if (inputCsvTimeList != null && !inputCsvTimeList.isEmpty()
									&& inputCsvTimeList.size() > (j-1)) {
								inputPdfTime = inputCsvTimeList.get(j - 1);	
							}
							
							if (stopName.equals(stopListName) && !traversed[i] && inputPdfTime.equals(gtfs2pdfMappedTime) ) {
								System.out.println(inputPdfTime);
								foundIndex = i;
								break;
							}
						}
					}
					
					if (foundIndex > -1) {
						output[foundIndex + 1][j] = gtfs2pdfMappedTime;
						
						String outputStopName = stopsMap.get(id);
						
						if (stops.get(foundIndex).contains(exUrbanArrivalSymbol)) {
							outputStopName = outputStopName + exUrbanArrivalSymbol;
							
						} else if (stops.get(foundIndex).contains(exUrbanDepartureSymbol)) {
							outputStopName = outputStopName + exUrbanDepartureSymbol;
						} 
						
						output[foundIndex + 1][0] = outputStopName + ";" + id;
						
						if (isAnomalyStop) {
							output[foundIndex + 1][0] = "*" + output[foundIndex + 1][0];
						}
						traversed[foundIndex] = true;
					} else {
						output[foundIndex + 1][0] = id + ";";
					}

					/** else simply following code works. **/
					/*output[stops.indexOf(stopListName) + 1][j] = stoptimeseq.get(gtfsSeq)[1].substring(0,
							time.lastIndexOf(":"));*/
				}

				// rerun for arrival times.
				for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

					boolean isAnomalyStop = false;
					String gtfsTime = stoptimeseq.get(gtfsSeq)[1];
					String gtfs2pdfMappedTime = gtfsTime.substring(0, gtfsTime.lastIndexOf(":")).trim();
					
					String id = stoptimeseq.get(gtfsSeq)[3];
					String stopListName = stopsMap.get(id).toLowerCase();
					
					String anomalyKey = id + "_" + gtfs2pdfMappedTime; 
					if (anomalyStopIds.contains(anomalyKey)) {
						isAnomalyStop = true;	
					}
					
					/** logic for handling cyclic trips for e.g. A_C festivo.**/
					int foundIndex = -1;
					for (int i = 0; i < stops.size(); i++) {
						
						// logic for taking stopName of final List.
						String stopName = stops.get(i);	
						if (stops.get(i).contains(exUrbanArrivalSymbol)) {
							String[] stopMatchPart = stops.get(i).split(exUrbanArrivalSymbol);
							stopName = stopMatchPart[0];
							
						} else if (stops.get(i).contains(exUrbanArrivalSymbol)) {
							String[] stopMatchPart = stops.get(i).split(exUrbanArrivalSymbol);
							stopName = stopMatchPart[0];
						} 
						
						if (isAnomalyStop) {
							if (stopName.equals(stopListName) && !traversed[i]) {
								foundIndex = i;
								break;
							}
							
						} else {
							List<String> inputCsvTimeList = null;
							if (inputCSVTimes.get(i) != null) {
								inputCsvTimeList = inputCSVTimes.get(i);
							}
							
							String inputPdfTime = "";
							if (inputCsvTimeList != null && !inputCsvTimeList.isEmpty()
									&& inputCsvTimeList.size() > (j-1)) {
								inputPdfTime = inputCsvTimeList.get(j - 1);	
							}
							
							if (stopName.equals(stopListName) && !traversed[i] && inputPdfTime.equals(gtfs2pdfMappedTime) ) {
								System.out.println(inputPdfTime);
								foundIndex = i;
								break;
							}
						}
						
					}
					if (foundIndex > -1) {
						output[foundIndex + 1][j] = gtfs2pdfMappedTime;
						
						String outputStopName = stopsMap.get(id);
						if (stops.get(foundIndex).contains(exUrbanArrivalSymbol)) {
							outputStopName = outputStopName + exUrbanArrivalSymbol;
							
						} else if (stops.get(foundIndex).contains(exUrbanDepartureSymbol)) {
							outputStopName = outputStopName + exUrbanDepartureSymbol;
						} 
						
						output[foundIndex + 1][0] = outputStopName + ";" + id;
							
						if (isAnomalyStop) {
							output[foundIndex + 1][0] = "*" + output[foundIndex + 1][0];
						}
						
						traversed[foundIndex] = true;
					} else {
						output[foundIndex + 1][0] = id + ";";
					}
					/** else simply following code works. **/
					/*output[stops.indexOf(stopListName) + 1][j] = stoptimeseq.get(gtfsSeq)[1].substring(0,
							time.lastIndexOf(":"));*/
				}

			}

			// fill in italic entries.
			for (String italicEntry : columnItalicStopNames.get(j)) {
				String name = italicEntry.substring(0, italicEntry.indexOf("$"));
				String time = italicEntry.substring(italicEntry.indexOf("$") + 1);
				output[stops.indexOf(name) + 1][j] = time;
			}
		}
		
		// stops column.
		output[0][0] = "stops;stop_id";


//		for (int i = 0; i < stops.size(); i++) {
//
//			String pdfStopMatchGTFS = stops.get(i);
//
//			if (stops.get(i).contains(exUrbanArrivalSymbol)) {
//				String[] stopMatchPart = stops.get(i).split(exUrbanArrivalSymbol);
//				pdfStopMatchGTFS = stopMatchPart[0];
//
//			} else if (stops.get(i).contains(exUrbanDepartureSymbol)) {
//				String[] stopMatchPart = stops.get(i).split(exUrbanDepartureSymbol);
//				pdfStopMatchGTFS = stopMatchPart[0];
//			}
//
//			if (stopIdsMap.containsKey(pdfStopMatchGTFS)) {
//				String stopId = stopIdsMap.get(pdfStopMatchGTFS);
//				if (stopsMap.containsKey(stopId)) {
//					output[i + 1][0] = stops.get(i) + ";" + stopId;
//					if (anomalyStopIds.contains(stopId)) {
//						output[i + 1][0] = "*" + output[i + 1][0];
//					}
//				} else {
//					output[i + 1][0] = stops.get(i) + ";" + stopId;
//					if (anomalyStopIds.contains(stopId)) {
//						output[i + 1][0] = "*" + output[i + 1][0];
//					}
//				}
//
//			} else {
//				output[i + 1][0] = stops.get(i) + ";";
//			}
//		}

		for (int col = 1; col < noOfOutputCols - 1; col++) {
			output[0][col] = fillHeaderAnnotation(stops, col);
		}

		output = clean(output);

		return output;

	}

	private List<List<String>> createListOfList(String[][] matrix, int numOfHeaders, int noOfCols) {

		List<List<String>> csvList = new ArrayList<List<String>>();

		for (int i = numOfHeaders; i < matrix.length; i++) {

			List<String> temp = new ArrayList<String>();
			for (int j = 1; j < noOfCols; j++) {
				if (matrix[i][j] != null
						&& !matrix[i][j].contains("|")
						&& !matrix[i][j].contains("|")
						&& !matrix[i][j].isEmpty()) {
					String pdfTime = matrix[i][j].replace(".", ":");
					int startTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));
					String formattedTime = formatter.format(startTimeHour) + pdfTime.substring(pdfTime.indexOf(":")).trim();
					temp.add(formattedTime);
				} else {
					temp.add("");
				}
			}

			csvList.add(i - numOfHeaders, temp);
		}

		return csvList;
	}

	/**
	private String[][] processUrbanMatrix(String[][] matrix, int noOfOutputCols) {

		// version 1.
		//for (int i = numOfHeaders; i < matrix.length; i++) {
		//	for (int j = 1; j < noOfOutputCols; j++) {
		//		output[i - numOfHeaders + 1][j] = matrix[i][j];
		//	}
		//	System.out.println(Arrays.toString(output[i]));
		}

		// create list of stops taking in to consideration GTFS data.
		List<String> stops = new ArrayList<String>();
		if (agencyId.equalsIgnoreCase("17")) {
			stops = processExUrbStops(matrix, numOfHeaders, noOfOutputCols - 1);
		} else {
			stops = processStops(matrix, numOfHeaders, noOfOutputCols - 1);
		}

		int noOfOutputRows = (stops.size() + 1);
		String[][] output = new String[noOfOutputRows][noOfOutputCols];
		// stops column.
		output[0][0] = "stops;stop_id";

		for (int j = 1; j < noOfOutputCols - 1; j++) {
			if (columnTripIdMap.containsKey(j)) {
				List<String[]> stoptimeseq = tripStopsTimesMap.get(columnTripIdMap.get(j).get(0));
				boolean traversed[] = new boolean[stops.size()];
				for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

					String time = stoptimeseq.get(gtfsSeq)[1];
					String id = stoptimeseq.get(gtfsSeq)[3];
					String stopListName = stopsMap.get(id).toLowerCase();
					// logic for handling cyclic trips for e.g. A_C festivo.
					int foundIndex = -1;
					for (int i = 0; i < stops.size(); i++) {
						if (stops.get(i).equals(stopListName) && !traversed[i]) {
							foundIndex = i;
							break;
						}
					}
					if (foundIndex > -1) {
						output[foundIndex + 1][j] = stoptimeseq.get(gtfsSeq)[1].substring(0, time.lastIndexOf(":"));
						traversed[foundIndex] = true;
					}

					//else simply following code works.
					
				}

			}

			// fill in italic entries.
			for (String italicEntry : columnItalicStopNames.get(j)) {
				String name = italicEntry.substring(0, italicEntry.indexOf("$"));
				String time = italicEntry.substring(italicEntry.indexOf("$") + 1);
				output[stops.indexOf(name) + 1][j] = time;
			}
		}

		//version 1.
		//for (int i = numOfHeaders; i < matrix.length; i++) {
		//	output[i - numOfHeaders + 1][0] = processStopsColumns(matrix[i][0]);
		//}

		//for (int j = 1; j < noOfOutputCols - 1; j++) {
		//	output[0][j] = mapTOGTFS(matrix, output, numOfHeaders, j);
		//}

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
	} **/

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
			annotation = annotation + "$" + note;
		}

		// TODO Auto-generated method stub
		return annotation;
	}

	/**
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
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()
						&& !matrix[i][currentCol].contains("|")) {
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
					tripStartIndex = i;
					break;
				}
			}
			int tripEndIndex = -1;
			for (int i = matrix.length - 1; i >= startRow; i--) {
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()
						&& !matrix[i][currentCol].contains("|")) {
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

			
			routeId = getGTFSRouteIdFromRouteShortName(routeShortName);

			if (matrix[5][currentCol] != null && !matrix[5][currentCol].isEmpty()) {
				String lineInfo = HtmlToText.htmlToPlainText(matrix[5][currentCol]);
				if (lineInfo.contains("Linea")) {
					String pdfRouteId = matrix[5][currentCol].substring(matrix[5][currentCol].indexOf('a') + 1);
					// check if xx/ routeId exist, else look for xx routeId.
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					if (routeId.isEmpty()) {
						routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
						if (routeId != null && !routeId.isEmpty()) {
							columnGTFSRSName.put(currentCol, pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
						}
					}
					mergedRoute = true;
				} else if (isInteger(lineInfo)) {
					String pdfRouteId = matrix[5][currentCol];
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					mergedRoute = true;
				} else if (roveretoNBuses.contains(lineInfo)) { // rovereto.
					String pdfRouteId = lineInfo;
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					mergedRoute = true;
				}
			}
	

			if (tripStartIndex > -1 && tripEndIndex > -1) {
				
				String startTime = matrix[tripStartIndex][currentCol].replace(".", ":");
				
				int startTimeHour = Integer.valueOf(startTime.substring(0, startTime.indexOf(":")));
				
				if (startTimeHour > 24) {
					startTimeHour = startTimeHour - 24;
				}
				startTime = formatter.format(startTimeHour) + startTime.substring(startTime.indexOf(":")).trim();

				String endTime = matrix[tripEndIndex][currentCol].replace(".", ":");

				int endTimeHour = Integer.valueOf(endTime.substring(0, endTime.indexOf(":")));

				if (endTimeHour > 24) {
					endTimeHour = endTimeHour - 24;
				}
				endTime = formatter.format(endTimeHour) + endTime.substring(endTime.indexOf(":")).trim();
				
				System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
						+ startTime + "-" + endTime + "]");
				
				// total csv trips counter.
				totalCSVTrips++;

				if (routeId != null && !routeId.isEmpty()) {

					List<String> tripsForRoute = routeTripsMap.get(routeId);

					if (tripsForRoute.isEmpty()) {
						System.err.println("no route found");
						columnNotes.add(ROUTE_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

					List<String> matchingTripId = new ArrayList<String>();
					for (String tripId : tripsForRoute) {
						List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

						if (stopTimes.get(0)[2].contains(startTime)
								&& stopTimes.get(stopTimes.size() - 1)[2].contains(endTime)) {

							if (mergedRoute) {
								if (!matchingTripId.contains(tripId)) {
									matchingTripId.add(tripId);
//									break;
								}
							} else {
								if (matchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
									if (!matchingTripId.contains(tripId)) {
										matchingTripId.add(tripId);
									}
//									break;
								}
							}

						}
					}

					
					if (matchingTripId == null || matchingTripId.isEmpty()) {
						// algorithm to check trip in other route.
						String tripId = partialTripMatchAlgo(matrix, currentCol, startRow, routeId);
						if (tripId != null && !tripId.isEmpty())
							matchingTripId.add(tripId);
					}
					
					
					// prepare stops list.
					if (matchingTripId != null && !matchingTripId.isEmpty()) {

						successMatch++;
						
						if (!matchedTripIds.contains(matchingTripId.get(0))) {
							matchedTripIds.add(matchingTripId.get(0));
//							successMatch++;
						}

						columnTripIdMap.put(currentCol, matchingTripId);

						if (mergedRoute && columnGTFSRSName.containsKey(currentCol)) {
							columnNotes.add(GTFS_RS_NAME + "=" + columnGTFSRSName.get(currentCol).trim());
						}

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
										&& !matrix[i + numOfHeaders][currentCol].contains("|")
										&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {

									pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";

									int pdfTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));

									if (pdfTimeHour > 24) {
										pdfTimeHour = pdfTimeHour - 24;

									}
									pdfTime = formatter.format(pdfTimeHour) + pdfTime.substring(pdfTime.indexOf(":")).trim();

								}
								stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(),
										stoptimeseq.get(gtfsSeq)[3]);
								if (pdfStopName.equalsIgnoreCase(gtfsStopName)
										&& stoptimeseq.get(gtfsSeq)[2].equalsIgnoreCase(pdfTime)
										&& stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
									stopList.set(i, stopsMap.get(stoptimeseq.get(gtfsSeq)[3]));
									found = true;
									System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
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
								System.err.println( "anamoly - " +  stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
							}
						}

					} else {
						System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
						columnNotes.add(TRIP_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";

					}

				} else {
					System.err.println("\n\n\\n--- perhaps no time defined in pdf ---");
					failedMatch++;
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
						anomalyStopIds.add(stoptimeseq.get(anamoly)[3]);
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
	} **/

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

				if (stopTimes.get(0)[2].contains(startTime) && stopTimes.get(stopTimes.size() - 1)[2].contains(endTime)) {

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
						if (stoptimeseq.get(s)[2].contains(timeToCheck) && !sequenceTraversed[s]) {
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

			int timeToCheckHour = Integer.valueOf(timeToCheck.substring(0, timeToCheck.indexOf(":")));

			if (timeToCheckHour > 24) {
				timeToCheckHour = timeToCheckHour - 24;
			}
			timeToCheck = formatter.format(timeToCheckHour) + timeToCheck.substring(timeToCheck.indexOf(":"));
			
			boolean found = false;
			/** to make sure if sequence time checked once. **/
			boolean[] tripSequence = new boolean[stopTimes.size()];

			/** very important (pdf seems to contain time mapped to departure time in stoptimes.txt.)
			 *  stopTimes.get(s)[2] departure time.
			 *  stopTimes.get(s)[1] arrival time.**/
			for (int s = 0; s < stopTimes.size(); s++) {
				
//				if (stopTimes.get(s)[2].contains(timeToCheck) && stopTimes.get(s)[1].contains(timeToCheck) && !tripSequence[s]) {
//					found = true;
//					tripSequence[s] = true;
//					break;
//				} else if (stopTimes.get(s)[1].contains(timeToCheck) && !tripSequence[s]) {  // only arrival time matches.
//					found = true;
//					break;
//				}
				if (stopTimes.get(s)[2].contains(timeToCheck) && !tripSequence[s]) {
					found = true;
					tripSequence[s] = true;
					break;
				}

			}
			if (!found) {
				System.err.println("probably misaligned GTFS time, compare tripId: " + stopTimes.get(0)[0]
						+ " times with PDF");
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
			if (agencyRoutesList.contains(linesTrip.get(i)[0])) {
				List<String> list = routeTripsMap.get(linesTrip.get(i)[0]);
				if (list == null) {
					list = new ArrayList<String>();
					routeTripsMap.put(linesTrip.get(i)[0], list);
				}
				list.add(linesTrip.get(i)[2]);
				if (!gtfsTripIds.contains(linesTrip.get(i)[2])) {
					gtfsTripIds.add(linesTrip.get(i)[2]);
				}
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

	private void deepFixMode() throws IOException {
		List<String> annotated = new ArrayList<String>();
		for (String fileName : fileColumnMismatchMap.keySet()) {
			if (!fileColumnMismatchMap.get(fileName).equalsIgnoreCase("")) {
				String fileInputName = fileName.substring(0, fileName.indexOf("-annotated"));
				String columnsToFix = fileColumnMismatchMap.get(fileName);
				File fileInput = new File(pathToInput + fileInputName + ".csv");
				File generatedCSV = new File(pathToOutput + fileName);
				List<String> lines = Files.asCharSource(fileInput, Charsets.UTF_8).readLines();
				annotated.addAll(deepConvertLines(lines, columnsToFix, generatedCSV));
				File annotatedCSV = new File(pathToOutput, fileName);
				Files.asCharSink(annotatedCSV, Charsets.UTF_8).writeLines(annotated);
			}
		}

	}

	private List<String> deepConvertLines(List<String> lines, String columnsToFix, File generatedCSV)
			throws IOException {

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

		/** create local copy of table as string[][] matrix. **/
		String[][] matrixInput = new String[lines.size()][maxNumberOfCols + 1];
		for (int i = 0; i < lines.size(); i++) {
			String tableString = "";
			if (table[i].length > 1) {
				for (int j = 0; j < table[i].length; j++) {
					tableString = tableString + table[i][j];
				}
			} else {
				tableString = table[i][0];
			}
			String[] colValues = tableString.split(";");
			for (int j = 0; j < colValues.length; j++) {
				matrixInput[i][j] = colValues[j];
			}
		}

		List<String> stepOneOutputLines = Files.asCharSource(generatedCSV, Charsets.UTF_8).readLines();
		/** read as table. **/
		String[][] generatedTable = new String[stepOneOutputLines.size()][];
		for (int i = 0; i < lines.size(); i++) {
			generatedTable[i] = StringUtils.commaDelimitedListToStringArray(stepOneOutputLines.get(i));
		}

		/** create local copy of table as string[][] matrix. **/
		String[][] stepOneOutputMatrix = new String[lines.size()][maxNumberOfCols + 1];
		for (int i = 0; i < lines.size(); i++) {
			String tableString = "";
			if (generatedTable[i].length > 1) {
				for (int j = 0; j < generatedTable[i].length; j++) {
					tableString = tableString + generatedTable[i][j];
				}
			} else {
				tableString = generatedTable[i][0];
			}
			String[] colValues = tableString.split(";");
			for (int j = 0; j < colValues.length; j++) {
				stepOneOutputMatrix[i][j] = colValues[j];
			}
		}

		// fix columns.
		String[] noOfcols = columnsToFix.split(",");
		for (int c = 0; c < noOfcols.length; c++) {
			if (noOfcols[c] != null && !noOfcols[c].isEmpty() && isInteger(noOfcols[c])) {
				String annotation = processColumn(matrixInput, Integer.valueOf(noOfcols[c]), numOfHeaders);
				if (annotation != null && !annotation.isEmpty()) {
					System.out.println("fixing column " + Integer.valueOf(noOfcols[c]));
					stepOneOutputMatrix[numOfHeaders][Integer.valueOf(noOfcols[c]) - 1] = annotation;
				}
			}
		}

		return converted;

	}
	
	private String partialTripMatchAlgo(String[][] matrix, int colInPdf, int startRow, String routeId) {
	
		String partialTripId = "";

		System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

		//		routeId = getGTFSRouteIdFromRouteShortName(routeShortName);
		//
		//		if (matrix[5][colInPdf] != null && matrix[5][colInPdf].contains("Linea")) {
		//			String pdfRouteId = matrix[5][colInPdf].substring(matrix[5][colInPdf].indexOf('a') + 1);
		//			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
		//			if (routeId.isEmpty()) {
		//				routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
		//				if (routeId != null && !routeId.isEmpty()) {
		//					columnGTFSRSName.put(colInPdf, pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
		//				}
		//			}
		//		} else if (matrix[5][colInPdf] != null && isInteger(matrix[5][colInPdf])) {
		//			String pdfRouteId = matrix[5][colInPdf];
		//			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
		//		}

		// validate trip with GTFS.
		boolean[] toBeCheckTimeIndex = new boolean[matrix.length];

		for (int i = startRow; i < matrix.length; i++) {

			if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty() && !matrix[i][colInPdf].contains("|")) {
				if (matrix[i][colInPdf].contains("-")) {
					continue;
				}
				toBeCheckTimeIndex[i] = true;
			}
		}

		if (routeId != null && !routeId.isEmpty()) {

			List<String> tripsForRoute = routeTripsMap.get(routeId);

			if (tripsForRoute.isEmpty()) {
				partialTripId = "no route found";
				return partialTripId;
			}

			int count = 0;
			for (Boolean boolT : toBeCheckTimeIndex) {
				if (boolT) {
					count++;
				}
			}

			List<String> matchingTripId = new ArrayList<String>();

			for (String tripId : tripsForRoute) {
				List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

				boolean foundPdfTime = false;
				boolean timeChecks[] = new boolean[count];

				for (int t = startRow, tbc = 0; t < toBeCheckTimeIndex.length; t++) {
					if (toBeCheckTimeIndex[t] && matrix[t][colInPdf] != null && !matrix[t][colInPdf].isEmpty()
							&& !matrix[t][colInPdf].contains("|")) {
						String timeToCheck = matrix[t][colInPdf].replace(".", ":");
						int timeToCheckHour = Integer.valueOf(timeToCheck.substring(0, timeToCheck.indexOf(":")));

						if (timeToCheckHour > 24) {
							timeToCheckHour = timeToCheckHour - 24;

						}
						timeToCheck = formatter.format(timeToCheckHour)
								+ timeToCheck.substring(timeToCheck.indexOf(":")).trim();
						System.out.println("check all trips for time " + matrix[t][colInPdf]);
						for (int s = 0; s < stopTimes.size(); s++) {
							// matches arrival or departure time (since pdf contains both entries).
							if (stopTimes.get(s)[1].contains(timeToCheck) | stopTimes.get(s)[2].contains(timeToCheck)) {
								foundPdfTime = true;
								timeChecks[tbc] = true;
								tbc++;
								break;
							}
						}
						if (!foundPdfTime) {
							break;
						}
					}
				}

				if (foundPdfTime) {
					boolean foundTrip = true;
					// check if all found.
					for (Boolean index : timeChecks) {
						if (!index) {
							foundTrip = false;

						}
					}

					// found
					if (foundTrip) {
						matchingTripId.add(tripId);
						break;
					}
				}

			}

			if (matchingTripId != null && !matchingTripId.isEmpty()) {

				if (matchingTripId.size() == 1) {
					System.out.println("found partial matched trip Id " + matchingTripId.get(0));
					partialTripId = matchingTripId.get(0);
				} else {
					System.err.println("anamoly- mutliple trips detected");
					for (String tripId : matchingTripId) {
						partialTripId = partialTripId + "-" + tripId;
					}
				}
			}
		}

		return partialTripId;

	}
	
	
	private String processColumn(String[][] matrix, int currentCol, int startRow) {

		String annotation = "";
		boolean italics = false;
		// mapping to input csv (addition of stopId column)
		int colInPdf = currentCol - 2;

		System.out.println("Processing column starting with time: " +  matrix[numOfHeaders][colInPdf]);
		
		routeId = getGTFSRouteIdFromRouteShortName(routeShortName);

		if (matrix[5][colInPdf] != null && matrix[5][colInPdf].contains("Linea")) {
			String pdfRouteId = matrix[5][colInPdf].substring(matrix[5][colInPdf].indexOf('a') + 1);
			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
		} else if (matrix[5][colInPdf] != null && isInteger(matrix[5][colInPdf])) {
			String pdfRouteId = matrix[5][colInPdf];
			routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
		}

		// validate trip with GTFS.
		boolean[] toBeCheckTimeIndex = new boolean[matrix.length];

		for (int i = startRow; i < matrix.length; i++) {

			if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty()) {
				if (matrix[i][colInPdf].contains("-")) {
					italics = true;
					continue;
				}
				toBeCheckTimeIndex[i] = true;
			}
		}

		if (routeId != null && !routeId.isEmpty()) {

			List<String> tripsForRoute = routeTripsMap.get(routeId);

			if (tripsForRoute.isEmpty()) {
				annotation = "no route found";
				return annotation;
			}

			int count = 0;
			for (Boolean boolT: toBeCheckTimeIndex) {
				if (boolT) {
					count++;
				}
			}
			
			List<String> matchingTripId = new ArrayList<String>();
			
			for (String tripId : tripsForRoute) {
				List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

				boolean foundPdfTime = false;
				boolean timeChecks[] = new boolean[count];
				
				for (int t = startRow, tbc = 0; t < toBeCheckTimeIndex.length; t++) {
					if (toBeCheckTimeIndex[t] && matrix[t][colInPdf] != null && !matrix[t][colInPdf].isEmpty()) {
						String timeToCheck = matrix[t][colInPdf].replace(".", ":");
						System.out.println("check all trips for time " + matrix[t][colInPdf]);
						for (int s = 0; s < stopTimes.size(); s++) {
							// matches arrival or departure time (since pdf contains both entries).
							if (stopTimes.get(s)[1].contains(timeToCheck)
									| stopTimes.get(s)[2].contains(timeToCheck)) {
								foundPdfTime = true;
								timeChecks[tbc] = true;
								tbc++;
								break;
							}
						}
						if (!foundPdfTime) {
							break;
						}
					}
				}

				if (foundPdfTime) {
					boolean foundTrip = true;
					// check if all found.
					for (Boolean index : timeChecks) {
						if (!index) {
							foundTrip = false;

						}
					}

					// found
					if (foundTrip) {
						matchingTripId.add(tripId);
						break;
					}
				}

			}

			if (matchingTripId != null && !matchingTripId.isEmpty()) {

				if (matchingTripId.size() == 1) {
					System.out.println("improved situation.... found trip Id " + matchingTripId.get(0));
					annotation = matchingTripId.get(0);
					if (!deepMatchedTripIds.contains(matchingTripId.get(0))) {
						deepMatchedTripIds.add(matchingTripId.get(0));	
					}
				} else {
					System.err.println("anamoly- mutliple trips detected");
					for (String tripId : matchingTripId) {
						annotation = annotation + "-" + tripId;

					}
				}
			} else {
				System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][colInPdf]);
				annotation = "no trip found";
			}
		} else {
			System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][colInPdf]);
			annotation = "no route found";
		}

		// notes(if any).
		if (italics) {
			annotation = annotation + " * italic entry found.";
		}

		return annotation;
	}

	private void printStats() {
		// TODO Auto-generated method stub
		if (stats) {
			System.out.println("\n\n\n\n");
			System.out.println("---------- WARNINGS ----------");
			for (String fileName : fileColumnMismatchMap.keySet()) {

				if (!fileColumnMismatchMap.get(fileName).equalsIgnoreCase("")) {
					System.out.println("check pdf " + fileName + " for columns " + fileColumnMismatchMap.get(fileName));
				}

			}

			System.out.println("-----------------------------");
			System.out.println("\n\n\n\n");
			//stats.
			if (routeStats) {
			System.out.println("%%%%%%%%%% RUN STATS %%%%%%%%%%");
			System.out.println("successful matches: " + successMatch);
			System.out.println("failed matches: " + failedMatch);
			System.out.println("success rate: " +  (successMatch / (successMatch + failedMatch)) * 100);
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			}
			
			if (csvStats ) {
				System.out.println("%%%%%%%%%% CSV STATS %%%%%%%%%%");
				System.out.println("total csv trips: " + totalCSVTrips);
				System.out.println("csv trips covered by GTFS: " + successMatch);
				System.out.println("csv trips not covered by GTFS: " + failedMatch);
				System.out.println("csv coverage: " +  (successMatch / (totalCSVTrips)) * 100);
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				
			}
			if (gtfsStats) {
			System.out.println("\n\n\n\n");
			System.out.println("%%%%%%%%%%%%%%% GTFS STATS %%%%%%%%%%%%%%");
			System.out.println("total number of GTFS trips for routes: " + gtfsTripIds.size());
			System.out.println("total number of matched trips for routes: " + matchedTripIds.size());
			System.out.println("coverage(normal) : " +  (Double.valueOf(matchedTripIds.size()) / Double.valueOf(gtfsTripIds.size())) * 100);
			if (deepMode) {
				System.out.println("Fixes in deep search mode :" + deepMatchedTripIds.size());
				System.out.println("coverage(deep) : " +  (Double.valueOf(matchedTripIds.size() + deepMatchedTripIds.size()) / Double.valueOf(gtfsTripIds.size())) * 100);
			}
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			System.out.println("\n\n\n\n");
			List<String> deltaTrips = new ArrayList<String>();
			deltaTrips.addAll(gtfsTripIds);
			deltaTrips.removeAll(deepMatchedTripIds);
			deltaTrips.removeAll(matchedTripIds);
			System.out.println("Trips Delta size :" + deltaTrips.size());
			for (String tripId: deltaTrips) {
				System.out.println(tripId);
			}
			}
			
		}
	}
	
	private List<String> processStops(String[][] matrix, int startRow, int noOfCols, List<List<String>> inputPdfTimes) {

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

			boolean mergedRoute = false;
			// additional notes for column map.
			List<String> columnNotes = new ArrayList<String>();
			columnHeaderNotes.put(currentCol, columnNotes);

			// column italic stopNames.
			List<String> italicStopEntry = new ArrayList<String>();
			columnItalicStopNames.put(currentCol, italicStopEntry);

			// total csv trips counter.
			totalCSVTrips++;
			
			int tripStartIndex = -1;
			for (int i = startRow; i < matrix.length; i++) {
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()
						&& !matrix[i][currentCol].contains("|")) {
					if (matrix[i][currentCol].contains("-")) {
						//						italics = true;
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
					tripStartIndex = i;

					// set arrival time flag.
					if (matrix[i][0].contains(" - Arr.")) {
						continue;
					}
					break;
				}
			}
			int tripEndIndex = -1;
			for (int i = matrix.length - 1; i >= startRow; i--) {
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()
						&& !matrix[i][currentCol].contains("|")) {
					if (matrix[i][currentCol].contains("-")) {
						//						italics = true;
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
					if (matrix[i][0].contains(" - Arr.")) {
						continue;
					}
					break;
				}
			}

			routeId = getGTFSRouteIdFromRouteShortName(routeShortName);

			if (matrix[5][currentCol] != null && !matrix[5][currentCol].isEmpty()) {
				String lineInfo = HtmlToText.htmlToPlainText(matrix[5][currentCol]);
				if (lineInfo.contains("Linea")) {
					String pdfRouteId = matrix[5][currentCol].substring(matrix[5][currentCol].indexOf('a') + 1);
					// check if xx/ routeId exist, else look for xx routeId.
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					if (routeId.isEmpty()) {
						routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
						if (routeId != null && !routeId.isEmpty()) {
							columnGTFSRSName.put(currentCol, pdfRouteId.substring(0, pdfRouteId.indexOf("/")));
						}
					}
					mergedRoute = true;
				} else if (isInteger(lineInfo)) {
					String pdfRouteId = matrix[5][currentCol];
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					mergedRoute = true;
				} else if (roveretoNBuses.contains(lineInfo)) { // rovereto.
					String pdfRouteId = lineInfo;
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					mergedRoute = true;
				}
			}

			// check for unaligned routes.
			if (routeId.isEmpty() && unalignedRoutesMap.containsKey(routeShortName)) {
				routeId = getGTFSRouteIdFromRouteShortName(unalignedRoutesMap.get(routeShortName));
			}

			if (routeId != null && !routeId.isEmpty()) {

				if (tripStartIndex > -1 && tripEndIndex > -1) {

					String startTime = matrix[tripStartIndex][currentCol].replace(".", ":");

					int startTimeHour = Integer.valueOf(startTime.substring(0, startTime.indexOf(":")));

					if (startTimeHour > 24) {
						startTimeHour = startTimeHour - 24;
					}

					startTime = formatter.format(startTimeHour) + startTime.substring(startTime.indexOf(":")).trim();

					String endTime = matrix[tripEndIndex][currentCol].replace(".", ":");

					int endTimeHour = Integer.valueOf(endTime.substring(0, endTime.indexOf(":")));

					if (endTimeHour > 24) {
						endTimeHour = endTimeHour - 24;

					}

					endTime = formatter.format(endTimeHour) + endTime.substring(endTime.indexOf(":")).trim();

					System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId
							+ "[" + startTime + "-" + endTime + "]");

					List<String> tripsForRoute = routeTripsMap.get(routeId);

					if (tripsForRoute.isEmpty()) {
						System.err.println("no route found");
						columnNotes.add(ROUTE_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

					List<String> matchingTripId = new ArrayList<String>();

					String foundTripId = null;
					for (String tripId : tripsForRoute) {
						List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

						if (stopTimes.get(0)[2].contains(startTime)
								&& stopTimes.get(stopTimes.size() - 1)[2].contains(endTime)) {

							foundTripId = tripId;
							break;

						}
					}

					if (foundTripId != null && !foundTripId.isEmpty()) {
						List<String[]> stopTimes = tripStopsTimesMap.get(foundTripId);
						if (mergedRoute) {
							/** first version(trip matching algorithm. **/
							if (!matchingTripId.contains(foundTripId)) {
								matchingTripId.add(foundTripId);
							}
						} else {
							if (deepMatchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
								if (!matchingTripId.contains(foundTripId)) {
									matchingTripId.add(foundTripId);
								}
							}
						}
					}

					if (matchingTripId == null || matchingTripId.isEmpty()) {
						// algorithm to check trip in other route.
						String tripId = partialTripMatchAlgo(matrix, currentCol, startRow, routeId);
						if (tripId != null && !tripId.isEmpty()) {
							matchingTripId.add(tripId);
						} 
//						else { //rerun with 90% match.
//							tripId = partialTripMatchByPercentAlgo(matrix, currentCol, startRow, routeId, 90);
//							if (tripId != null && !tripId.isEmpty()) {
//								matchingTripId.add(tripId);
//							}
//						}
					}

					// check trains.
					if (matchingTripId == null || matchingTripId.isEmpty()) {
						// algorithm to check trip in other route.
						String tripId = checkTrainTrips(matrix, currentCol, startRow);
						if (tripId != null && !tripId.isEmpty())
							matchingTripId.add(tripId);
					}

					// prepare stops list.
					if (matchingTripId != null && !matchingTripId.isEmpty()) {

						successMatch++;

						if (!matchedTripIds.contains(matchingTripId.get(0))) {
							matchedTripIds.add(matchingTripId.get(0));
							//	successMatch++;
						}

						columnTripIdMap.put(currentCol, matchingTripId);

						if (mergedRoute && columnGTFSRSName.containsKey(currentCol)) {
							columnNotes.add(GTFS_RS_NAME + "=" + columnGTFSRSName.get(currentCol).trim());
						}

						List<String[]> stoptimeseq = tripStopsTimesMap.get(matchingTripId.get(0));

						// RUN FIRST FOR DEPARTURE TIMES.
						for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

							boolean found = false;
							String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "")
									.toLowerCase();

							for (int i = 0; i < pdfStopList.size(); i++) {
								boolean appendDepartureString = false;
								// pdf sequence = i + numOfHeaders;
								String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
								pdfStopName = pdfStopName.replaceAll("\"", "");
								pdfStopName = pdfStopName.replace(" (", "-");
								pdfStopName = pdfStopName.replace(")", "");

								if (pdfStopName.contains(exUrbanArrivalSymbol)) {
									continue;

								}

								if (pdfStopName.contains(exUrbanDepartureSymbol)) {
									pdfStopName = pdfStopName.replace(exUrbanDepartureSymbol, "").trim();
									appendDepartureString = true;

								}

								String pdfTime = "";
								if (matrix[i + numOfHeaders][currentCol] != null
										&& !matrix[i + numOfHeaders][currentCol].contains("|")
										&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {
									pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";

									int pdfTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));

									if (pdfTimeHour > 24) {
										pdfTimeHour = pdfTimeHour - 24;
									}

									pdfTime = formatter.format(pdfTimeHour)
											+ pdfTime.substring(pdfTime.indexOf(":")).trim();
								}
								stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(),
										stoptimeseq.get(gtfsSeq)[3]);

								if (pdfStopName.equalsIgnoreCase(gtfsStopName)
										&& stoptimeseq.get(gtfsSeq)[2].contains(pdfTime)
										&& stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {

									String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]);
									if (appendDepartureString) {
										stopNameInList = stopNameInList + exUrbanDepartureSymbol;
										appendDepartureString = false;
									}
									stopList.set(i, stopNameInList);
									found = true;
									// System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
									break;
								}
							}

							if (!found && !mergedRoute &&stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
								anamolies = anamolyMap.get(matchingTripId.get(0));
								if (anamolies == null) {
									anamolies = new ArrayList<Integer>();
									anamolyMap.put(matchingTripId.get(0), anamolies);
								}
								anamolies.add(gtfsSeq); // adding sequence number.	
								//	System.err.println( "anamoly - " +  stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
							}
						}

						// run again for arrival.
						for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

							boolean found = false;
							String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "")
									.toLowerCase();

							for (int i = 0; i < pdfStopList.size(); i++) {
								boolean isArrival = false;
								boolean appendDepartureString = false;
								// pdf sequence = i + numOfHeaders;
								String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
								pdfStopName = pdfStopName.replaceAll("\"", "");
								pdfStopName = pdfStopName.replace(" (", "-");
								pdfStopName = pdfStopName.replace(")", "");

								if (pdfStopName.contains(exUrbanArrivalSymbol)) {
									pdfStopName = pdfStopName.replace(exUrbanArrivalSymbol, "").trim();
									isArrival = true;

								} else {
									continue;
								}

								String pdfTime = "";
								if (matrix[i + numOfHeaders][currentCol] != null
										&& !matrix[i + numOfHeaders][currentCol].contains("|")
										&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {
									pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";

									int pdfTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));

									if (pdfTimeHour > 24) {
										pdfTimeHour = pdfTimeHour - 24;
									}
									pdfTime = formatter.format(pdfTimeHour)
											+ pdfTime.substring(pdfTime.indexOf(":")).trim();
								}
								stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(),
										stoptimeseq.get(gtfsSeq)[3]);

								if (isArrival) {
									if (pdfStopName.equalsIgnoreCase(gtfsStopName)
											&& stoptimeseq.get(gtfsSeq)[1].contains(pdfTime)
											&& stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
										String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3])
												+ exUrbanArrivalSymbol;
										stopList.set(i, stopNameInList);
										found = true;
										isArrival = false;
										// System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
										break;
									}

								} else {

									if (pdfStopName.equalsIgnoreCase(gtfsStopName)
											&& stoptimeseq.get(gtfsSeq)[2].contains(pdfTime)
											&& stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {

										String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]);
										if (appendDepartureString) {
											stopNameInList = stopNameInList + exUrbanDepartureSymbol;
											appendDepartureString = false;
										}
										stopList.set(i, stopNameInList);
										found = true;
										// System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
										break;
									}
								}
							}

							if (!found && !mergedRoute &&  stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
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
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

				} else {
					System.err.println("\n\n\\n--- perhaps no time defined in pdf ---");
					failedMatch++;
				}

			} else {
				System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][currentCol]);
				columnNotes.add(ROUTE_ERROR);
				failedMatch++;
				mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
			}
		}

		// refactored names.
		for (int i = 0; i < stopList.size(); i++) {
			String stop = stopList.get(i);
			if (stopList.contains(stop + exUrbanArrivalSymbol) | stopList.contains(stop + exUrbanDepartureSymbol)) {
				continue;
			}
			String pdfStopName = stop.replaceAll("\\s+", " ");
			System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
			stopList.set(i, pdfStopName.toLowerCase());

		}
		
		List<String> handledAnomalyStops = new ArrayList<String>();
		// adding anamolies.
		for (String tripId : anamolyMap.keySet()) {

			List<Integer> anamoliesList = anamolyMap.get(tripId);
			List<String[]> stoptimeseq = tripStopsTimesMap.get(tripId);

//			if (tripId.equalsIgnoreCase("0001796202015062720150909")) {
//				System.err.println("0001796202015062720150909");
//			}
			for (int anamoly : anamoliesList) {
				String stopToBeAdded = stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase();
				System.out.println("trying to add stop " + stopToBeAdded + ":"
						+ stoptimeseq.get(anamoly)[3]);
				String stopNameBefore = null;
				for (int a = anamoly - 1; a > -1; a--) {
					stopNameBefore = stopsMap.get(stoptimeseq.get(a)[3]).toLowerCase();
					System.err.println(stopNameBefore + "-" + stoptimeseq.get(a)[3]);
					if (stopNameBefore != null && !stopNameBefore.isEmpty() && stopList.lastIndexOf(stopNameBefore) != -1) {
						break;
					}
				}

				if (stopList.indexOf(stopToBeAdded + exUrbanArrivalSymbol) != -1
						|| stopList.indexOf(stopToBeAdded + exUrbanDepartureSymbol) != -1) {
					continue;
				}
				
				if (stopList.lastIndexOf(stopNameBefore + exUrbanDepartureSymbol) != -1) {
					int insertIndex = stopList.lastIndexOf(stopNameBefore + exUrbanDepartureSymbol) + 1;
					String stopName = stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase();
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.add(stoptimeseq.get(anamoly)[3] + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim());
					if (!handledAnomalyStops.contains(stopName)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase());
						pdfStopList.add(insertIndex, "*"); // to align with modified stopList.
						inputPdfTimes.add(insertIndex, new ArrayList<String>());
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopName);
					}
				} else if (stopList.lastIndexOf(stopNameBefore) != -1) {
					int insertIndex = stopList.lastIndexOf(stopNameBefore) + 1;
					String stopName = stopsMap.get(stoptimeseq.get(anamoly)[3]);
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.add(stoptimeseq.get(anamoly)[3] + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim());
					if (!handledAnomalyStops.contains(stopName)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase());
						pdfStopList.add(insertIndex, "*"); // to align with modified stopList.
						inputPdfTimes.add(insertIndex, new ArrayList<String>());
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopName);
					}
				}
			}

		}

//		List<String> stopsFinal = new ArrayList<String>();

//		// remove duplicate stops.
//		for (String stop : stopList) {
//			if (stopList.contains(stop + exUrbanArrivalSymbol) | stopList.contains(stop + exUrbanDepartureSymbol)) {
//				continue;
//			}
////			if (stopIdsMap.containsKey(stop)) {
////				stopsFinal.add(stop.toLowerCase());
////			} else {
//				String pdfStopName = stop.replaceAll("\\s+", " ");
//				System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
////				if (!stopsFinal.contains(stop.toLowerCase()))
//				stopsFinal.add(pdfStopName.toLowerCase());
////			}
//		}

		return stopList;
//		return stopsFinal;
		
	}

    private String checkTrainTrips(String[][] matrix, int colInPdf, int startRow) {
    	String trainTripId = "";

		System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

		// validate trip with GTFS.
		boolean[] toBeCheckTimeIndex = new boolean[matrix.length];

		for (int i = startRow; i < matrix.length; i++) {

			if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty() && !matrix[i][colInPdf].contains("|")) {
				if (matrix[i][colInPdf].contains("-")) {
					continue;
				}

				if (matrix[i][0].contains(" - Arr.")) {
					continue;
				}

				toBeCheckTimeIndex[i] = true;
			}
		}

		for(String trainRouteId: exUrbTrenoRoutes) {

			List<String> tripsForRoute = routeTripsMap.get(trainRouteId);

			if (tripsForRoute != null && !tripsForRoute.isEmpty()) {
				int count = 0;
				for (Boolean boolT : toBeCheckTimeIndex) {
					if (boolT) {
						count++;
					}
				}

				List<String> matchingTripId = new ArrayList<String>();

				for (String tripId : tripsForRoute) {
					List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

					boolean foundPdfTime = false;
					boolean timeChecks[] = new boolean[count];

					for (int t = startRow, tbc = 0; t < toBeCheckTimeIndex.length; t++) {
						if (toBeCheckTimeIndex[t] && matrix[t][colInPdf] != null && !matrix[t][colInPdf].isEmpty()
								&& !matrix[t][colInPdf].contains("|")) {
							String timeToCheck = matrix[t][colInPdf].replace(".", ":");
							int timeToCheckHour = Integer.valueOf(timeToCheck.substring(0, timeToCheck.indexOf(":")));

							if (timeToCheckHour > 24) {
								timeToCheckHour = timeToCheckHour - 24;

							}

							timeToCheck = formatter.format(timeToCheckHour)
									+ timeToCheck.substring(timeToCheck.indexOf(":")).trim();

							System.out.println("check all trips for time " + matrix[t][colInPdf]);
							for (int s = 0; s < stopTimes.size(); s++) {
								if (stopTimes.get(s)[2].contains(timeToCheck) | stopTimes.get(s)[2].contains(timeToCheck)) {
									foundPdfTime = true;
									timeChecks[tbc] = true;
									tbc++;
									break;
								}
							}
							if (!foundPdfTime) {
								break;
							}
						}
					}

					if (foundPdfTime) {
						boolean foundTrip = true;
						// check if all found.
						for (Boolean index : timeChecks) {
							if (!index) {
								foundTrip = false;

							}
						}

						// found
						if (foundTrip) {
							matchingTripId.add(tripId);
							break;
						}
					}

				}

				if (matchingTripId != null && !matchingTripId.isEmpty()) {

					if (matchingTripId.size() == 1) {
						System.out.println("found partial matched trip Id " + matchingTripId.get(0));
						trainTripId = matchingTripId.get(0);
					} else {
						System.err.println("anamoly- mutliple trips detected");
						for (String tripId : matchingTripId) {
							trainTripId = trainTripId + "-" + tripId;
						}
					}
				}				
			} else {
				return trainTripId;
			}
		}

		return trainTripId;
	}

	private String partialTripMatchByPercentAlgo(String[][] matrix, int colInPdf, int startRow, String routeId, int percentage) {
		
		String partialTripId = "";

		System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

		// validate trip with GTFS.
		boolean[] toBeCheckTimeIndex = new boolean[matrix.length];

		for (int i = startRow; i < matrix.length; i++) {

			if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty() && !matrix[i][colInPdf].contains("|")) {
				if (matrix[i][colInPdf].contains("-")) {
					continue;
				}

				if (matrix[i][0].contains(" - Arr.")) {
					continue;
				}

				toBeCheckTimeIndex[i] = true;
			}
		}

		if (routeId != null && !routeId.isEmpty()) {

			List<String> tripsForRoute = routeTripsMap.get(routeId);

			if (tripsForRoute.isEmpty()) {
				partialTripId = "no route found";
				return partialTripId;
			}

			int count = 0;
			for (Boolean boolT : toBeCheckTimeIndex) {
				if (boolT) {
					count++;
				}
			}

			List<String> matchingTripId = new ArrayList<String>();

			for (String tripId : tripsForRoute) {
				List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

				boolean foundPdfTime = false;
				boolean timeChecks[] = new boolean[count];

				for (int t = startRow, tbc = 0; t < toBeCheckTimeIndex.length; t++) {
					if (toBeCheckTimeIndex[t] && matrix[t][colInPdf] != null && !matrix[t][colInPdf].isEmpty()
							&& !matrix[t][colInPdf].contains("|")) {
						String timeToCheck = matrix[t][colInPdf].replace(".", ":");
						int timeToCheckHour = Integer.valueOf(timeToCheck.substring(0, timeToCheck.indexOf(":")));

						if (timeToCheckHour > 24) {
							timeToCheckHour = timeToCheckHour - 24;

						}

						timeToCheck = formatter.format(timeToCheckHour)
								+ timeToCheck.substring(timeToCheck.indexOf(":")).trim();

						System.out.println("check all trips for time " + matrix[t][colInPdf]);
						for (int s = 0; s < stopTimes.size(); s++) {
							if (stopTimes.get(s)[2].contains(timeToCheck) | stopTimes.get(s)[2].contains(timeToCheck)) {
								foundPdfTime = true;
								timeChecks[tbc] = true;
								tbc++;
								break;
							}
						}
						if (Double.valueOf(tbc)/Double.valueOf(count) * 100 >= percentage) {
							foundPdfTime = true;
							break;
							
						}
						if (!foundPdfTime) {
							break;
						}
					}
				}

				if (foundPdfTime && percentage == 100) {
					boolean foundTrip = true;
					// check if all found.
					for (Boolean index : timeChecks) {
						
						if (!index) {
							foundTrip = false;

						}
					}
					// found
					if (foundTrip) {
						matchingTripId.add(tripId);
						break;
					}
				} else {
					matchingTripId.add(tripId);
					break;
				}

			}

			if (matchingTripId != null && !matchingTripId.isEmpty()) {

				if (matchingTripId.size() == 1) {
					System.out.println("found partial matched trip Id " + matchingTripId.get(0));
					partialTripId = matchingTripId.get(0);
				} else {
					System.err.println("anamoly- mutliple trips detected");
					for (String tripId : matchingTripId) {
						partialTripId = partialTripId + "-" + tripId;
					}
				}
			}
		}

		return partialTripId;

	}
	
	private boolean deepMatchTrips(String[][] matrix, int currentCol, int tripStartIndex, int tripEndIndex,
			List<String[]> stopTimes) {

		int i = 0;
		for (i = tripStartIndex; i <= tripEndIndex; i++) {

//			boolean isArrivalTime = false;

			if (matrix[i][currentCol] == null || matrix[i][currentCol].isEmpty() || matrix[i][currentCol].contains("|")
					|| matrix[i][currentCol].contains("-")) {
				continue;

			}
			
			// set arrival time flag.
			if (matrix[i][0].contains(" - Arr.")) {
				continue;
			}

			String timeToCheck = matrix[i][currentCol].replace(".", ":");

			// set arrival time flag.
//			if (matrix[i][0].contains(" - Arr.")) {
//				isArrivalTime = true;
//			}

			int timeToCheckHour = Integer.valueOf(timeToCheck.substring(0, timeToCheck.indexOf(":")));

			if (timeToCheckHour > 24) {
				timeToCheckHour = timeToCheckHour - 24;
			}
			
			timeToCheck = formatter.format(timeToCheckHour) + timeToCheck.substring(timeToCheck.indexOf(":")).trim();

			boolean found = false;
			/** to make sure if sequence time checked once. **/
			boolean[] tripSequence = new boolean[stopTimes.size()];

			/** very important (pdf seems to contain time mapped to departure time in stoptimes.txt.)
			 *  stopTimes.get(s)[2] departure time.
			 *  stopTimes.get(s)[1] arrival time.**/
			for (int s = 0; s < stopTimes.size(); s++) {

				if (stopTimes.get(s)[2].contains(timeToCheck) && !tripSequence[s]) {
						found = true;
						tripSequence[s] = true;
						break;
					}
				

			}
			if (!found) {
				System.err.println("probably misaligned GTFS time, compare tripId: " + stopTimes.get(0)[0]
						+ " times with PDF");
				return false;
			}
		}

		if (i == tripEndIndex + 1) {
			return true;
		}

		return false;

	}

	public static void main(String[] args) throws Exception {
		AnnotatedTTGenerator timeTableGenerator = new AnnotatedTTGenerator();
//		File folder = new File(pathToInput);
//
//		for (final File fileEntry : folder.listFiles()) {
//			if (fileEntry.isDirectory() | fileEntry.getName().contains(".json")) {
//				continue;
//			} else {
//				System.out.println("Annotation in process for ->  " + fileEntry.getName());
//				timeTableGenerator.processFiles(pathToOutput, agencyId, pathToInput + fileEntry.getName());
//			}
//		}

//		timeTableGenerator.processFiles(pathToOutput, "12", pathToInput + "05A-Feriale.csv");
//		timeTableGenerator.processFiles(pathToOutput, "16", pathToInput + "E-03R-Feriale.csv");
		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "632R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "209A-R.csv");

		timeTableGenerator.printStats();

		// hard fix mode.
		if (deepMode) {

			timeTableGenerator.deepFixMode();

			timeTableGenerator.destroy();

			timeTableGenerator.printStats();
		}

	}

}
