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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.StringUtils;

import sayservice.FileRouteModel.FileRouteAgencyModel;

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
	// verbose.
	private static boolean verbose = false;
	// err.
	private static boolean err = false;
	// input GTFS.
	private static final String pathToGTFS = "src/test/resources/gtfs/12/";
//	private static final String pathToGTFS = "src/test/resources/gtfs/16/";
//	private static final String pathToGTFS = "src/test/resources/gtfs/17/";
//	 output folder.
	private static final String pathToOutput = "src/test/resources/annotatedtimetable/12/";
//	private static final String pathToOutput = "src/test/resources/annotatedtimetable/16/";
//	private static final String pathToOutput = "src/test/resources/annotatedtimetable/17/";
	// input folder.
	private static final String pathToInput = "src/test/resources/inputtimetable/12/";
//	private static final String pathToInput = "src/test/resources/inputtimetable/16/";
//	private static final String pathToInput = "src/test/resources/inputtimetable/17/";
	// agencyIds (12,16,17)
	private static final String agencyId = "12";
	private static final List<String> roveretoNBuses = Arrays.asList("N1", "N2", "N3", "N5", "N6");
	private static final List<String> exUrbTrenoRoutes = Arrays.asList("578", "518", "352");
	private static final Map<String, List<String>> unalignedRoutesMap = new HashMap<String, List<String>>();
	{
		unalignedRoutesMap.put("104", new ArrayList<>(Arrays.asList("101")));
		unalignedRoutesMap.put("119", new ArrayList<>(Arrays.asList("109", "110")));
		unalignedRoutesMap.put("120", new ArrayList<>(Arrays.asList("102", "103", "112")));
		unalignedRoutesMap.put("122", new ArrayList<>(Arrays.asList("501")));
		unalignedRoutesMap.put("131", new ArrayList<>(Arrays.asList("101")));
		unalignedRoutesMap.put("108", new ArrayList<>(Arrays.asList("112")));
		unalignedRoutesMap.put("201", new ArrayList<>(Arrays.asList("204", "205")));
		unalignedRoutesMap.put("204", new ArrayList<>(Arrays.asList("201", "205")));
		unalignedRoutesMap.put("205", new ArrayList<>(Arrays.asList("201", "204")));
		unalignedRoutesMap.put("206", new ArrayList<>(Arrays.asList("204", "236")));
		unalignedRoutesMap.put("231", new ArrayList<>(Arrays.asList("201", "215")));
		unalignedRoutesMap.put("245", new ArrayList<>(Arrays.asList("215")));
		unalignedRoutesMap.put("321", new ArrayList<>(Arrays.asList("306")));
		unalignedRoutesMap.put("301", new ArrayList<>(Arrays.asList("332", "335")));
		unalignedRoutesMap.put("303", new ArrayList<>(Arrays.asList("306")));
		unalignedRoutesMap.put("306", new ArrayList<>(Arrays.asList("303")));
		unalignedRoutesMap.put("307", new ArrayList<>(Arrays.asList("334")));
		unalignedRoutesMap.put("332", new ArrayList<>(Arrays.asList("301")));
		unalignedRoutesMap.put("334", new ArrayList<>(Arrays.asList("301", "335")));
		unalignedRoutesMap.put("335", new ArrayList<>(Arrays.asList("301", "334")));
		unalignedRoutesMap.put("403", new ArrayList<>(Arrays.asList("402")));
		unalignedRoutesMap.put("461", new ArrayList<>(Arrays.asList("403")));
		unalignedRoutesMap.put("462", new ArrayList<>(Arrays.asList("417", "418")));
		unalignedRoutesMap.put("463", new ArrayList<>(Arrays.asList("401")));
		unalignedRoutesMap.put("464", new ArrayList<>(Arrays.asList("423")));
		unalignedRoutesMap.put("467", new ArrayList<>(Arrays.asList("115", "401", "403", "303")));
		unalignedRoutesMap.put("501", new ArrayList<>(Arrays.asList("506", "512")));
		unalignedRoutesMap.put("503", new ArrayList<>(Arrays.asList("501", "506")));
		unalignedRoutesMap.put("511", new ArrayList<>(Arrays.asList("501", "503", "506", "514")));
		unalignedRoutesMap.put("645", new ArrayList<>(Arrays.asList("646")));
		unalignedRoutesMap.put("646", new ArrayList<>(Arrays.asList("640", "642", "645")));
		unalignedRoutesMap.put("Servizio Extraurbano", new ArrayList<>(Arrays.asList("627")));
	}
	private static final String exUrbanArrivalSymbol = "- arr.";
	private static final String exUrbanDepartureSymbol = "- part.";
	private static final String UTF8_BOM = "\uFEFF";
	private static final String ITALIC_ENTRY = "italic";
	private static final String ROUTE_ERROR = "route not found";
	private static final String TRIP_ERROR = "trip not found";
	private static final String GTFS_RS_NAME = "GTFS_RS_Name";
	private static int numOfHeaders = 7;
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
	private static Map<String, List<String>> tripRouteServiceHeadsignIdMap = new HashMap<String, List<String>>();
	private Map<String, String> tripServiceIdMap = new HashMap<String, String>();
	private HashMap<String, String> stopsMap = new HashMap<String, String>();
	private HashMap<Integer, List<String>> columnTripIdMap = new HashMap<Integer, List<String>>();
	private List<String> unAlignedTripIds = new ArrayList<String>();
	private HashMap<Integer, List<String>> columnHeaderNotes = new HashMap<Integer, List<String>>();
	private HashMap<Integer, List<String>> columnItalicStopNames = new HashMap<Integer, List<String>>();
	private HashMap<String, String> stopIdsMap = new HashMap<String, String>();
	private HashMap<Integer, String> columnGTFSRSName = new HashMap<Integer, String>();

	private List<String[]> routes;

	private List<String> anamolyStopIdMap = new ArrayList<String>();

	private Map<String, Integer> anomalyStopIds = new HashMap<String, Integer>();

	// stats variables.
	private static double failedMatch = 0;
	private static double successMatch = 0;
	private static Map<String, String> fileColumnMismatchMap = new HashMap<String, String>();
	private static List<String> agencyRoutesList = new ArrayList<String>();
	private static List<String> matchedTripIds = new ArrayList<String>();
	private static List<String> deepMatchedTripIds = new ArrayList<String>();
	private static List<String> gtfsTripIds = new ArrayList<String>();
	private int totalCSVTrips;
	private static int ignoredTrips;

	String mismatchColIds = "";

	private List<String> ignoreServiceIdPattern = new ArrayList<String>() {
		{
			// urban.
			add("2015062620150909");
			add("2015061020150909");
			// ex-urban(ignore winter).
//			add("20160624");
//			add("20160329");
//			add("20160106");
//			add("20160607");
//			add("20151031");
//			add("20151222");
			// ex-urban (ignore summer.)
//			add("20150909");
//			add("20150831");
			
		}
	};
	  
	// urban.
	private String outputPattern = "2015091020160607";
	// ex-urban.
//	private String outputPattern = "2015091020160624"; //2015091020160624,2015062620150909
	
	private Map<String, boolean[]> calendarEntries = new HashMap<String, boolean[]>();
	private Map<String, List<String>> serviceIdMapping = new HashMap<String, List<String>>();
	private static final String CALENDAR_ALLDAYS = "AD";
	private static final String CALENDAR_LUNVEN = "LV";
	private static final String CALENDAR_LUNSAB = "LS";
	private static final String CALENDAR_FESTIVO = "F";
	private static final String CALENDAR_SOLOSAB = "SS";
	private static final String CALENDAR_SOLOVEN = "SV";
	private static final String CALENDAR_SOLMERCOLEDI = "SMERC";
	private static final String CALENDAR_SOLGIOV = "SGIOV";
	private Map<String, List<String>> serviceIdExcepType1 = new HashMap<String, List<String>>();
	private Map<String, List<String>> serviceIdExcepType2 = new HashMap<String, List<String>>();
	
	private static final Map<String, List<String>> serviceExceptionType1Dates = new HashMap<String, List<String>>();
	{
	}
	
	private static final Map<String, List<String>> serviceExceptionType2Dates = new HashMap<String, List<String>>();
	{
		serviceExceptionType2Dates.put("scolastica da lunedì a sabato", new ArrayList<>(Arrays.asList("20151223","20160101")));
		serviceExceptionType2Dates.put("scolastica da lunedì a venerdì", new ArrayList<>(Arrays.asList("20151223","20160101")));
		serviceExceptionType2Dates.put("scolastica solo il sabato", new ArrayList<>(Arrays.asList("20160102"))); //sabato di non-scolastic period.
		serviceExceptionType2Dates.put("non scol. da lunedì a sabato", new ArrayList<>(Arrays.asList("20150911")));
	}
	
	private static final Map<String, String> pdfFreqStringServiceIdMap = new HashMap<String, String>();
	{
		// scolastici services.
		pdfFreqStringServiceIdMap.put("scolastica da lunedì a sabato", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("scolastica da lunedì a venerdì", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("Scolastica solo il Sabato", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("non scol. da lunedì a sabato", CALENDAR_LUNSAB);
		// normal services.
		pdfFreqStringServiceIdMap.put("solo nei giorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale da lunedì a venerdì", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("feriale solo il sabato", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("solo nei giorni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei giorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale escluso sabato", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("solo al sabato feriale", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale escluso sab", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("solo al mercoledì feriale", CALENDAR_SOLMERCOLEDI);
		pdfFreqStringServiceIdMap.put("solo nei ggiorni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("solo al giovedì feriale", CALENDAR_SOLGIOV);
		pdfFreqStringServiceIdMap.put("solo nei girni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei ggiorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("orario festivo", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("orario feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("orario feriale", CALENDAR_LUNSAB);
//		"Postfestivo"
	}
	
	List<String> deleteList = new ArrayList<String>();
	
	private static List<String> fixedOrderList = new ArrayList<String>();
	
	private static RouteModel routeModel;

	private static String[] andataSuffix = new String[] { "A-annotated.csv", "a-annotated.csv",
			"A-Feriale-annotated.csv", "a-Feriale-annotated.csv", "A-Festivo-annotated", "a-Festivo-annotated" };

	private static String[] ritornoSuffix = new String[] { "R-annotated.csv", "r-annotated.csv",
			"R-Feriale-annotated.csv", "r-Feriale-annotated.csv", "R-Festivo-annotated", "r-Festivo-annotated" };

	private static List<String> ignoreServiceList = new ArrayList<String>() {
		{
			add("Treno Trenitalia");
			add("Bus SAD");
			add("Treno TTE");
		}
	};
	
	private static  List<String> servizioString = new ArrayList<String>();
	
	private static FileRouteModel fileRouteModel;
	
	private static  List<String> frequencyString = new ArrayList<String>();

	public AnnotatedTTGenerator() throws IOException {
		try {
			mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
			database = mongoClient.getDB("smart-planner-15x");
			collection = database.getCollection("stops");
			mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			init(agencyId);
			// route model read from configuration (remove)
			this.fileRouteModel = readFileRouteConfigurationModel();
			this.routeModel = readRouteModel();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void processFiles(String outputDir, String agency, String... files) throws Exception {
		List<String> annotated = new ArrayList<String>();
		for (String filename : files) {
			String outputName = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("."));
			
			//ex-urban.
			if (agencyId.equalsIgnoreCase("17")) {
				outputName = outputName.replace("-", "_");
			}
			outputName = outputName + "-" + outputPattern + "-annotated.csv";
			File file = new File(filename);
			List<String> lines = Files.asCharSource(file, Charsets.UTF_8).readLines();
			annotated.addAll(convertLines(lines, outputName));
			File outputDirFile = new File(outputDir);
			if (!outputDirFile.exists()) {
				outputDirFile.mkdir();
			}
			// rovereto.
			if (agencyId.equalsIgnoreCase("16")) {
				outputName = outputName.substring(outputName.indexOf("-") + 1);
			}
			
			
			File annotatedCSV = new File(outputDirFile, outputName);
			Files.asCharSink(annotatedCSV, Charsets.UTF_8).writeLines(annotated);

			fileColumnMismatchMap.put(outputName, mismatchColIds);
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
		unAlignedTripIds.clear();

	}

	private List<String> convertLines(List<String> lines, String outputFileName) throws Exception {

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
				// if (verbose) System.out.println(matrix[i][j]);
			}
		}

		// filter matrix, removing ignore services.
		String[][] matrixFiltered = new String[lines.size()][maxNumberOfCols + 1];

		// copy values until row 3.
		for (int row = 0; row < 3; row++) {
			for (int col = 0; col < maxNumberOfCols; col++) {
				matrixFiltered[row][col] = matrix[row][col];
			}
		}

		for (int col = 0, filterMatrixCol = 0; col < maxNumberOfCols; col++) {
			if (matrix[6][col] != null && !matrix[6][col].isEmpty()) {
				String spId = matrix[6][col].replace("\"", "");
				if (spId != null && !spId.isEmpty() && ignoreServiceList.contains(spId)) {
					continue;
				}
				if (!servizioString.contains(spId)) {
					servizioString.add(spId);
				}
			}

			for (int row = 3; row < matrix.length; row++) {
				matrixFiltered[row][filterMatrixCol] = matrix[row][col];
			}
			filterMatrixCol++;
		}

		/** write heading in output. **/
		for (int i = 0; i < numOfHeaders; i++) {
			String line = "";
			for (int col = 0; col < maxNumberOfCols; col++) {
				String cellValue = matrixFiltered[i][col]; 
				if (matrixFiltered[i][col] != null && !matrixFiltered[i][col].equalsIgnoreCase("null")) {
					cellValue = cellValue.replace("\"", "");
					line = line + cellValue + ";";
				}
			}
			converted.add(line.replaceFirst(";", ";;"));
		}
		
		// extract GTFS information and structures.
		routeShortName = matrixFiltered[0][1].replaceAll("\"","");
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
		output = processMatrix(matrixFiltered, noOfOutputCols, outputFileName);

		// consistency check.
		String[][] checkOutput = consistencyCheck(output, noOfOutputCols, outputFileName);
		
		output = checkOutput;
		
		// simple print existing matrix.
		for (int i = 0; i < output.length; i++) {
			String line = "";
			for (int j = 0; j < maxNumberOfCols; j++) {
				line = line + output[i][j] + ";";
			}
			// if (verbose) System.out.println(line);
			converted.add(line);
		}

		return converted;
	}

	
	private String[][] consistencyCheck(String[][] output, int noOfOutputCols, String outputFileName) {
		String[][] matrix = output;
		boolean inconsistent = false;
		int iRow = -1;
		int iCol = -1;
		//W
		try {
			for (int j = 1; j < noOfOutputCols - 1; j++) {
				for (int i = 4; i < (output.length - 1); i++) {
					String currTime = output[i][j];
					if (!currTime.isEmpty()) {
						Date curr = TIME_FORMAT.parse(currTime);
						for (int iNext = i + 1; iNext < output.length; iNext++) {
							String nextTime = output[iNext][j];
							if (!nextTime.isEmpty() && !nextTime.startsWith("-")) {
								Date next = TIME_FORMAT.parse(nextTime);
								if (curr.after(next)) {
									System.err.println(output[i][0]);
									inconsistent = true;
									iRow = i;
									iCol = j;
								}
								break;
							}

						}
						if (inconsistent) {
							break;
						}
					}
				}
				if (inconsistent) {
					break;
				}
			}
			
			int switchRow = -1;
			if (inconsistent) {
				// fix the order
				if (!fixedOrderList.contains(outputFileName)) {
					fixedOrderList.add(outputFileName);	
				}
				
				if (output[iRow][0].startsWith("*")) { // unimportant stop fix
					Date iTime = TIME_FORMAT.parse(output[iRow][iCol]);
					for (int f = iRow + 1; f < output.length; f++) {
						String next = output[f][iCol];
						if (!next.isEmpty() && !next.startsWith("-")) {
							Date nextTime = TIME_FORMAT.parse(next);
							if (nextTime.after(iTime)) {
								System.out.println(outputFileName + "time: " + iTime + " should be place before: " + nextTime + " (" + iRow + "," + iCol + ")");
								switchRow = f - 1;
								break;
							}
						}
					}
					
					if (switchRow == -1) {
						for (int f = iRow + 1; f < output.length; f++) {
							String next = output[f][iCol];
							if (!next.isEmpty() && !next.startsWith("-")) {
								Date nextTime = TIME_FORMAT.parse(next);
								if (nextTime.equals(iTime)) {
									System.out.println(outputFileName + "time: " + iTime + " should be place before: " + nextTime + " (" + iRow + "," + iCol + ")");
									switchRow = f - 1;
									break;
								}
							}
						}
					}
					
					if (switchRow == -1) { // its the last stop,put it after the next one. (04A-Festivo).
						switchRow = iRow + 1;
					}

					if (switchRow > -1) {
						String[][] revisedOutput = new String[output.length][noOfOutputCols];
						String[] temp = output[iRow];
						// copy until switch row skipping anomaly row.
						for (int a = 0; a < iRow; a++) {
							revisedOutput[a] = output[a];
						}
						for (int a = iRow; a < switchRow; a++) {
							revisedOutput[a] = output[a + 1];
						}
						// add anomaly row in correct(switch) position.
						revisedOutput[switchRow] = temp;
						// copy after switch position.
						for (int a = switchRow + 1; a < output.length; a++) {
							revisedOutput[a] = output[a];
						}

						matrix = consistencyCheck(revisedOutput, noOfOutputCols, outputFileName);
						
					}
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

		return matrix;
	}

	private boolean checkColumnTimes(String[][] output, int row, int col) {
		boolean inconsistent = false;
		try {
			Date curr = TIME_FORMAT.parse(output[row][col]);
			for (int i = row + 1; i < output.length; i++) {
				String nextTime = output[i][col];
				if (!nextTime.isEmpty()) {
					Date next = TIME_FORMAT.parse(nextTime);
					if (curr.after(next)) {
						inconsistent = true;
						break;
					}
				}
			}

		} catch (ParseException e) {
			System.err.println(e.getMessage());
		}
		return inconsistent;
	}

	private String[][] processMatrix(String[][] matrix, int noOfOutputCols, String outputFileName) {

		// create list of stops taking in to consideration GTFS data.
		List<String> stops = new ArrayList<String>();
		
		// prepare list of List<String> for input matrix.
		List<List<String>> inputCSVTimes = createListOfList(matrix, numOfHeaders, noOfOutputCols);
		
		stops = processStops(matrix, numOfHeaders, noOfOutputCols - 1, inputCSVTimes, outputFileName);

		int noOfOutputRows = (stops.size() + 5);
		String[][] output = new String[noOfOutputRows][noOfOutputCols];
		
		// stops column.
		output[0][0] = "gtfs trip_id;";
		output[1][0] = "smartplanner route_id;";
		output[2][0] = "service_id;";
		output[3][0] = "stops;stop_id";
		
		for (int i = 0; i < stops.size(); i++) {
			output[i+4][0] = stops.get(i) + ";";
		}
				
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
					if (anomalyStopIds.containsKey(anomalyKey) && anomalyStopIds.get(anomalyKey) == -1) {
						isAnomalyStop = true;
					}
					
					
					/** logic for handling cyclic trips for e.g. A_C festivo.**/
					int foundIndex = -1;
					for (int i = 0; i < stops.size(); i++) {
						
						// logic for taking stopName of final List.
						String stopName = stops.get(i); //.replace("\"", "");
						if (stops.get(i).contains(exUrbanDepartureSymbol)) {
							String[] stopMatchPart = stops.get(i).split(exUrbanDepartureSymbol);
							stopName = stopMatchPart[0]; 
						}
						
						if (isAnomalyStop) {
							if (stopName.contains("$")) {
								String[] stopNameParts = stopName.split("\\$"); 
								if (stopNameParts[0].equals(stopListName)
										&& id.equalsIgnoreCase(stopNameParts[1])
										&& !traversed[i]) {
									foundIndex = i;
									break;
								}
							}
						} else {
							List<String> inputCsvTimeList = null;
							if (inputCSVTimes.get(i) != null) {
								inputCsvTimeList = inputCSVTimes.get(i);
							}
							
							String inputPdfTime = "";
							if (inputCsvTimeList != null && !inputCsvTimeList.isEmpty()
									&& inputCsvTimeList.size() > (j - 1)) {
								inputPdfTime = inputCsvTimeList.get(j - 1);
								if (inputPdfTime != null && !inputPdfTime.isEmpty() && inputPdfTime.indexOf(":") != -1) {
									int pdfHour = Integer.valueOf(inputPdfTime.substring(0, inputPdfTime.indexOf(":")));
									if (pdfHour > 24) {
										pdfHour = pdfHour - 24;
									}

									inputPdfTime = formatter.format(pdfHour)
											+ inputPdfTime.substring(inputPdfTime.indexOf(":")).trim();
								}
							}
							
							if (stopName.equals(stopListName) && !traversed[i] && inputPdfTime.equals(gtfs2pdfMappedTime) ) {
								if (verbose) System.out.println(inputPdfTime);
								foundIndex = i;
								break;
							}
						}
					}
					
					
					if (foundIndex > -1) {
						output[foundIndex + 4][j] = gtfs2pdfMappedTime;
						
						String outputStopName = stopsMap.get(id);
						
						if (stops.get(foundIndex).contains(exUrbanArrivalSymbol)) {
							outputStopName = outputStopName + exUrbanArrivalSymbol;
							
						} else if (stops.get(foundIndex).contains(exUrbanDepartureSymbol)) {
							outputStopName = outputStopName + exUrbanDepartureSymbol;
						} 
						
						output[foundIndex + 4][0] = outputStopName + ";" + id;
						
						if (isAnomalyStop) {
							output[foundIndex + 4][0] = "*" + output[foundIndex + 4][0];
							anomalyStopIds.put(anomalyKey, 0);
						}
						traversed[foundIndex] = true;
					}

				}

				// rerun for arrival times.
				for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

					boolean isAnomalyStop = false;
					String gtfsTime = stoptimeseq.get(gtfsSeq)[1];
					String gtfs2pdfMappedTime = gtfsTime.substring(0, gtfsTime.lastIndexOf(":")).trim();
					
					String id = stoptimeseq.get(gtfsSeq)[3];
					String stopListName = stopsMap.get(id).toLowerCase();
					
					String anomalyKey = id + "_" + gtfs2pdfMappedTime; 
					if (anomalyStopIds.containsKey(anomalyKey) && anomalyStopIds.get(anomalyKey) == -1) {
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
						}
						
						if (isAnomalyStop) {
							if (stopName.contains("$")) {
								String[] stopNameParts = stopName.split("\\$"); 
								if (stopNameParts[0].equals(stopListName)
										&& id.equalsIgnoreCase(stopNameParts[1])
										&& !traversed[i]) {
									foundIndex = i;
									break;
								}
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
								if (inputPdfTime != null && !inputPdfTime.isEmpty() && inputPdfTime.indexOf(":") != -1) {
									int pdfHour = Integer.valueOf(inputPdfTime.substring(0, inputPdfTime.indexOf(":")));
									if (pdfHour > 24) {
										pdfHour = pdfHour - 24;
									}

									inputPdfTime = formatter.format(pdfHour)
											+ inputPdfTime.substring(inputPdfTime.indexOf(":")).trim();
								}
							}
							
							if (stopName.equals(stopListName) && !traversed[i] && inputPdfTime.equals(gtfs2pdfMappedTime) ) {
								if (verbose) System.out.println(inputPdfTime);
								foundIndex = i;
								break;
							}
						}
						
					}
					if (foundIndex > -1) {
						output[foundIndex + 4][j] = gtfs2pdfMappedTime;
						
						String outputStopName = stopsMap.get(id);
						if (stops.get(foundIndex).contains(exUrbanArrivalSymbol)) {
							outputStopName = outputStopName + exUrbanArrivalSymbol;
							
						} else if (stops.get(foundIndex).contains(exUrbanDepartureSymbol)) {
							outputStopName = outputStopName + exUrbanDepartureSymbol;
						} 
						
						output[foundIndex + 4][0] = outputStopName + ";" + id;
							
						if (isAnomalyStop) {
							output[foundIndex + 4][0] = "*" + output[foundIndex + 4][0];
						}
						
						traversed[foundIndex] = true;
					}

				}

			}

			// fill in italic entries.
			boolean[] italicEntered = new boolean[stops.size()];
			for (String italicEntry : columnItalicStopNames.get(j)) {
				String name = italicEntry.substring(0, italicEntry.indexOf("$"));
				String time = italicEntry.substring(italicEntry.indexOf("$") + 1);
//				output[stops.indexOf(name.toLowerCase()) + 4][j] = time.replace(".", ":");
//				String[] stopNameId = output[stops.indexOf(name.toLowerCase()) + 4][0].split(";");
//				if (stopNameId.length < 2) {
//					output[stops.indexOf(name.toLowerCase()) + 4][0] = name + ";";
//				}
				int stopIndex = -1;
				for (int i = 0; i < stops.size(); i++) {
					if (stops.get(i).equalsIgnoreCase(name) && !italicEntered[i]) {
						stopIndex = i;
						italicEntered[i] = true;
						break;
					}
				}
				if (stopIndex != -1) {
					output[stopIndex + 4][j] = time.replace(".", ":");
					String[] stopNameId = output[stopIndex + 4][0].split(";");
					if (stopNameId.length < 2) {
						output[stopIndex + 4][0] = name + ";";
					}
				}
				
			}
		}
		
//		// stops column.
//		output[0][0] = "stops;stop_id";


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
			output[0][col] = fillGTFSTripId(col);
			output[1][col] = fillSmartPlannerRouteId(stops, col, outputFileName);
			output[2][col] = fillServiceId(col);
			output[3][col] = fillHeaderAnnotation(stops, col);
		}

		output = clean(output);

		return output;

	}

	private String fillServiceId(int col) {
		String serviceId = "";

		List<String> tripIds = columnTripIdMap.get(col);
		String gtfsTripId = "";

		if (tripIds != null) {
			if (tripIds.size() == 1) {
				gtfsTripId = tripIds.get(0);
				if (tripServiceIdMap.containsKey(gtfsTripId)) {
					serviceId = tripServiceIdMap.get(gtfsTripId);
				}

			} else if (tripIds.size() > 1) {
				// multiple trips.
				for (String tripId : tripIds) {
					if (tripServiceIdMap.containsKey(tripId)) {
						serviceId = serviceId + tripServiceIdMap.get(tripId) + "$";
					}
				}
			}
		}

		return serviceId;
	}

	private String fillSmartPlannerRouteId(List<String> stops, int col, String fileName) {
		String cacheRouteId = "";
		
		
		List<String> tripIds = columnTripIdMap.get(col);
		String gtfsTripId = "";

		if (tripIds != null) {
			if (tripIds.size() == 1) {
				gtfsTripId = tripIds.get(0);
				if (tripRouteServiceHeadsignIdMap.containsKey(gtfsTripId)) {
					List<String> tripInfoGTFS = tripRouteServiceHeadsignIdMap.get(gtfsTripId);
					String directionId = tripInfoGTFS.get(3);
					String routeId = tripInfoGTFS.get(0);
					// if file name ends with A-annoated.csv. (ANDATA)
					for (String aSuffix : andataSuffix) {
						if (fileName.endsWith(aSuffix)) {
							directionId = "0";
						}
					}
					for (String rSuffix : ritornoSuffix) {
						if (fileName.endsWith(rSuffix)) {
							directionId = "1";
						}
					}
					if (Integer.valueOf(directionId) != Integer.valueOf(tripInfoGTFS.get(3))) {
						System.err.println("directionId different from GTFS for: " + fileName + " tripId: "
								+ gtfsTripId + "(gtfsDirectionId -> " + tripInfoGTFS.get(3) + ")");
					}
					// identify new routeId
					String key = routeId + "_" + agencyId + "_" + directionId;
					sayservice.RouteModel.AgencyModel am = routeModel.agency(agencyId);
					if (am.getRouteMappings() != null && am.getRouteMappings().containsKey(key)) {
						cacheRouteId = am.getRouteMappings().get(key);
					} else {
						cacheRouteId = key;
					}
				}
			} else if (tripIds.size() > 1) {
				// multiple trips.
				for (String tripId : tripIds) {
					if (tripRouteServiceHeadsignIdMap.containsKey(tripId)) {
						List<String> tripInfoGTFS = tripRouteServiceHeadsignIdMap.get(tripId);
						String directionId = tripInfoGTFS.get(3);
						String routeId = tripInfoGTFS.get(0);
						// if file name ends with A-annoated.csv. (ANDATA)
						for (String aSuffix : andataSuffix) {
							if (fileName.endsWith(aSuffix)) {
								directionId = "0";
							}
						}
						for (String rSuffix : ritornoSuffix) {
							if (fileName.endsWith(rSuffix)) {
								directionId = "1";
							}
						}
						if (Integer.valueOf(directionId) != Integer.valueOf(tripInfoGTFS.get(3))) {
							System.err.println("directionId different from GTFS for: " + fileName + " tripId: "
									+ gtfsTripId + "(gtfsDirectionId -> " + tripInfoGTFS.get(3) + ")");
						}
						// identify new routeId
						String key = routeId + "_" + agencyId + "_" + directionId;
						sayservice.RouteModel.AgencyModel am = routeModel.agency(agencyId);
						if (am.getRouteMappings() != null && am.getRouteMappings().containsKey(key)) {
							cacheRouteId = cacheRouteId + am.getRouteMappings().get(key) + "$";
						} else {
							cacheRouteId = cacheRouteId + key + "$";
						}
					}
				}
			}
		}

		return cacheRouteId;
	}

	
	private String fillGTFSTripId(int col) {

		List<String> tripIds = columnTripIdMap.get(col);
		String gtfsTripId = "";

		if (tripIds != null) {
			if (tripIds.size() == 1) {
				gtfsTripId = tripIds.get(0);
			} else if (tripIds.size() > 1) {
				// multiple trips.
				for (String tripId : tripIds) {
					gtfsTripId = gtfsTripId + tripId + "$";
					if (gtfsTripId.equalsIgnoreCase("0002695852015061020150909")) {
						System.err.println("arrived at breakpoint.");
					}
				}
			}
		}

		return gtfsTripId;
	
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
		//	if (verbose) System.out.println(Arrays.toString(output[i]));
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
				if (unAlignedTripIds.contains(tripIds.get(0)) && agencyId.equalsIgnoreCase("17")) {
					annotation = "*" + tripIds.get(0);
				} else { // exact
					annotation = tripIds.get(0);
				}
			} else if (tripIds.size() > 1) {
				boolean isUnalignedTrip = false;
				// multiple trips.
				for (String tripId : tripIds) {
					if (unAlignedTripIds.contains(tripId) && agencyId.equalsIgnoreCase("17")) {
						isUnalignedTrip = true;
					}
					annotation = annotation + tripId + "$";
				}

				if (isUnalignedTrip) {
					annotation = "*" + annotation;
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
				
				if (verbose) System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
						+ startTime + "-" + endTime + "]");
				
				// total csv trips counter.
				totalCSVTrips++;

				if (routeId != null && !routeId.isEmpty()) {

					List<String> tripsForRoute = routeTripsMap.get(routeId);

					if (tripsForRoute.isEmpty()) {
						if (err) System.err.println("no route found");
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
									if (verbose) System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
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
								if (err) System.err.println( "anamoly - " +  stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
							}
						}

					} else {
						if (err) System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
						columnNotes.add(TRIP_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";

					}

				} else {
					if (err) System.err.println("\n\n\\n--- perhaps no time defined in pdf ---");
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
				if (err) System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
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

		if (verbose) System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId + "["
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
					if (err) System.err.println("anamoly- mutliple trips detected");
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
									if (err) System.err.println("anamoly detected for stop id: " + "(" + stopId + ","
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
				if (err) System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
				annotation = "no trip found";
			}
		} else {
			if (err) System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][currentCol]);
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
				if (err) System.err.println("probably misaligned GTFS time, compare tripId: " + stopTimes.get(0)[0]
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
		//if (verbose) System.out.println(regexQuery.toString());
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
		String calendarFile = pathToGTFS + "calendar.txt";
		String calendarDatesFile = pathToGTFS + "calendar_dates.txt";
		//		HashMap<String, String> stopsMap = new HashMap<String, String>();

		List<String[]> linesTrip = readFileGetLines(tripFile);
		List<String[]> linesST = readFileGetLines(stoptimesTFile);
		List<String[]> stops = readFileGetLines(stopFile);
		List<String[]> calendar = readFileGetLines(calendarFile);
		List<String[]> calendarDates = readFileGetLines(calendarDatesFile);
		
		routes = readFileGetLines(routeFile);

		for (String[] words : routes) {
			if (!agencyRoutesList.contains(words[0]) & !(words[0].equalsIgnoreCase("route_id"))) {
				agencyRoutesList.add(words[0]);
			}
		}
		
		for (String[] words : calendar) {
			if (!words[0].equalsIgnoreCase("service_id")) {
				String serviceId = words[0];
				boolean b[] = new boolean[7];
				for (int i = 1; i < 8; i++) {
					b[i - 1] = words[i].equals("1") ? true : false;
				}
				calendarEntries.put(serviceId, b);
			}
		}
		
		// create mapping for serviceId in calendar_dates.
		for (String[] words : calendarDates) {
			if (!words[0].equalsIgnoreCase("service_id")) {
				String serviceId = words[0];
				if (serviceId.equalsIgnoreCase("0000000592015091020160607")) {
					System.out.println("");
				}
				// Exception Type 1(service added).
				List<String> datesExType1 = serviceIdExcepType1.get(serviceId);
				if (datesExType1 == null) {
					datesExType1 = new ArrayList<String>();
					serviceIdExcepType1.put(serviceId, datesExType1);
				}
				// Exception Type 2(service removed).
				List<String> datesExType2 = serviceIdExcepType2.get(serviceId);
				if (datesExType2 == null) {
					datesExType2 = new ArrayList<String>();
					serviceIdExcepType2.put(serviceId, datesExType2);
				}

				if (words[2].equalsIgnoreCase("1")) {
					datesExType1.add(words[1]);
				} else {
					datesExType2.add(words[1]);
				}
			}
		}
		
		// create mapping for calendar LV,F,AD,SS,LS
		for (String serviceId : calendarEntries.keySet()) {
			
			for (String ignoreServiceId : ignoreServiceIdPattern) {
				if (serviceId.endsWith(ignoreServiceId)) {
					continue;
				}
			}
			
			boolean[] b = calendarEntries.get(serviceId);
			List<String> serviceIds = null;
			if (b[0] & b[1] & b[2] & b[3] & b[4] & b[5] & b[6]) { //AD.
				serviceIds = serviceIdMapping.get(CALENDAR_ALLDAYS);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_ALLDAYS, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (b[0] & b[1] & b[2] & b[3] & b[4] & b[5] & !b[6]) { // LS
				serviceIds = serviceIdMapping.get(CALENDAR_LUNSAB);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_LUNSAB, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (b[0] & b[1] & b[2] & b[3] & b[4] & !b[5] & !b[6]) { // LV.
				serviceIds = serviceIdMapping.get(CALENDAR_LUNVEN);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_LUNVEN, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & !b[2] & !b[3] & !b[4] & b[5] & !b[6]) { // SS.
				serviceIds = serviceIdMapping.get(CALENDAR_SOLOSAB);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_SOLOSAB, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & !b[2] & !b[3] & !b[4] & !b[5] & b[6]) { //F
				serviceIds = serviceIdMapping.get(CALENDAR_FESTIVO);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_FESTIVO, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & !b[2] & !b[3] & b[4] & !b[5] & !b[6]) { // SV.
				serviceIds = serviceIdMapping.get(CALENDAR_SOLOVEN);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_SOLOVEN, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & b[2] & !b[3] & !b[4] & !b[5] & !b[6]) { //SMERC
				serviceIds = serviceIdMapping.get(CALENDAR_SOLMERCOLEDI);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_SOLMERCOLEDI, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & !b[2] & b[3] & !b[4] & !b[5] & !b[6]) { //SGIOV
				serviceIds = serviceIdMapping.get(CALENDAR_SOLGIOV);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_SOLGIOV, serviceIds);
				}
				serviceIds.add(serviceId);
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
		
		for (int i = 0; i < linesTrip.size(); i++) {
			tripServiceIdMap.put(linesTrip.get(i)[2], linesTrip.get(i)[1]);
		}
		
		for (int i = 0; i < linesTrip.size(); i++) {
			List<String> list = tripRouteServiceHeadsignIdMap.get(linesTrip.get(i)[0]);
			if (list == null) {
				list = new ArrayList<String>();
				tripRouteServiceHeadsignIdMap.put(linesTrip.get(i)[2], list);
			}
			list.add(linesTrip.get(i)[0]);
			list.add(linesTrip.get(i)[1]);
			list.add(linesTrip.get(i)[3]);
			list.add(linesTrip.get(i)[4]);
			list.add(linesTrip.get(i)[5]);
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
				if (verbose) System.out.println("Error parsing route: " + words[0] + "," + words[1] + "," + words[2]);
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
					if (verbose) System.out.println("fixing column " + Integer.valueOf(noOfcols[c]));
					stepOneOutputMatrix[numOfHeaders][Integer.valueOf(noOfcols[c]) - 1] = annotation;
				}
			}
		}

		return converted;

	}
	
	private List<String> partialTripMatchAlgo(String[][] matrix, int colInPdf, int startRow, String routeId, boolean isUnAlignedRoute) {
	
		List<String> matchingTripId = new ArrayList<String>();

		if (verbose)
			System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

		if (!isUnAlignedRoute) {

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
					return matchingTripId;
				}

				int count = 0;
				for (Boolean boolT : toBeCheckTimeIndex) {
					if (boolT) {
						count++;
					}
				}

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
							if (verbose)
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
							if (!matchingTripId.contains(foundTrip)) {
								matchingTripId.add(tripId);	
							}
						}
					}

				}
			}
		} else {

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

			for (String unAlignedRouteId : unalignedRoutesMap.get(routeShortName)) {

				routeId = getGTFSRouteIdFromRouteShortName(unAlignedRouteId);

				List<String> tripsForRoute = routeTripsMap.get(routeId);

				if (tripsForRoute.isEmpty()) {
					return matchingTripId;
				}

				int count = 0;
				for (Boolean boolT : toBeCheckTimeIndex) {
					if (boolT) {
						count++;
					}
				}

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
							if (verbose)
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
							if (!matchingTripId.contains(foundTrip)) {
								matchingTripId.add(tripId);	
							}
						}
					}

				}

			}

		}
		
		// filter old dates.
		if (matchingTripId.size() > 1) {
			List<String> copyOfSWTripIds = new ArrayList<String>();
			copyOfSWTripIds.addAll(matchingTripId);

			for (String matchId : copyOfSWTripIds) {

				String serviceId = tripServiceIdMap.get(matchId);

				for (String ignoreServiceId : ignoreServiceIdPattern) {
					if (serviceId.endsWith(ignoreServiceId)) {
						matchingTripId.remove(matchId);
					}
				}
			}
		}
		
		// validate service calendar.
		List<String> copyOfTripIds = new ArrayList<String>();
		copyOfTripIds.addAll(matchingTripId);
		
		if (copyOfTripIds != null && !copyOfTripIds.isEmpty()) {

			for (String matchId : copyOfTripIds) {
				
				// read frequency info.
				if (matrix[4][colInPdf] != null && !matrix[4][colInPdf].isEmpty()) {
					String pdfFreqString = matrix[4][colInPdf].replaceAll("\\s+", " ").toLowerCase();
					pdfFreqString = pdfFreqString.replace("\"","");
					if (pdfFreqStringServiceIdMap.containsKey(pdfFreqString)) {
						String servicIdMapIdentifier = pdfFreqStringServiceIdMap.get(pdfFreqString);
						
						String gtfsServiceId = tripServiceIdMap.get(matchId);
						
						//check if it is 'scolastici/non' service.
						if (serviceExceptionType2Dates.containsKey(servicIdMapIdentifier)
								| serviceExceptionType1Dates.containsKey(servicIdMapIdentifier)) {
							
							// match any exception type 1 day.
							List<String> dayOn = serviceIdExcepType1.get(gtfsServiceId);
							dayOn.retainAll(serviceExceptionType1Dates.get(servicIdMapIdentifier));
							
							// match any exception type 2 day.
							List<String> dayOff = serviceIdExcepType2.get(gtfsServiceId);
							dayOff.retainAll(serviceExceptionType2Dates.get(servicIdMapIdentifier));
							
							if (dayOff.size() < 0 && dayOn.size() < 0) {
								matchingTripId.remove(matchId);
								continue;
							}
						}
						
						List<String> serviceIds = serviceIdMapping.get(servicIdMapIdentifier);
						if (!serviceIds.contains(gtfsServiceId)) {
							matchingTripId.remove(matchId);
							continue;
						}
					}
				} 
			}
		}
			
		

		return matchingTripId;

	}
	
	
	private String processColumn(String[][] matrix, int currentCol, int startRow) {

		String annotation = "";
		boolean italics = false;
		// mapping to input csv (addition of stopId column)
		int colInPdf = currentCol - 2;

		if (verbose) System.out.println("Processing column starting with time: " +  matrix[numOfHeaders][colInPdf]);
		
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
						if (verbose) System.out.println("check all trips for time " + matrix[t][colInPdf]);
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
					if (verbose) System.out.println("improved situation.... found trip Id " + matchingTripId.get(0));
					annotation = matchingTripId.get(0);
					if (!deepMatchedTripIds.contains(matchingTripId.get(0))) {
						deepMatchedTripIds.add(matchingTripId.get(0));	
					}
				} else {
					if (err) System.err.println("anamoly- mutliple trips detected");
					for (String tripId : matchingTripId) {
						annotation = annotation + "-" + tripId;

					}
				}
			} else {
				if (err) System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][colInPdf]);
				annotation = "no trip found";
			}
		} else {
			if (err) System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][colInPdf]);
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
				System.out.println("csv trips ignored (merged routes): " + ignoredTrips);
				System.out.println("csv coverage: " +  (successMatch / (totalCSVTrips - ignoredTrips)) * 100);
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
	
	private List<String> processStops(String[][] matrix, int startRow, int noOfCols, List<List<String>> inputPdfTimes
			, String outputfileName) {

		// merged list of stops.
		List<String> stopList = new ArrayList<String>();
		// pdf list of stops.
		List<String> pdfStopList = new ArrayList<String>();
		List<Integer> anamolies = null;

		for (int i = 0; i < (matrix.length - numOfHeaders); i++) {
			String pdfStopName = matrix[i + numOfHeaders][0].trim();
//			pdfStopName = pdfStopName.replaceAll("\\s+", " ");
//			pdfStopName = pdfStopName.replace(" )", ")");
//			pdfStopName = pdfStopName.replace(" (", "(");
//			pdfStopName = pdfStopName.replace(". ", ".");
			if (agencyId.equalsIgnoreCase("17")) {
				pdfStopName = pdfStopName.replaceAll("\\s+", " ");
				pdfStopName = pdfStopName.replace(" )", ")");
				pdfStopName = pdfStopName.replace(" (", "(");
				pdfStopName = pdfStopName.replace(". ", ".");
				pdfStopName = pdfStopName.replaceAll("\"", "");	
			}
			pdfStopList.add(pdfStopName);
		}

		// add all pdf stop first to final list.
		stopList.addAll(pdfStopList);

		LinkedHashMap<String, List<Integer>> anamolyMap = new LinkedHashMap<String, List<Integer>>();

		for (int currentCol = 1; currentCol < noOfCols; currentCol++) {

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
						//						italics = true;
						if (!columnNotes.contains(ITALIC_ENTRY)) {
							columnNotes.add(ITALIC_ENTRY);
						}
						String stopName = matrix[i][0].replaceAll("\\s+", " ");
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
						String stopName = matrix[i][0].replaceAll("\\s+", " ");
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
			
			if (tripStartIndex > -1 | tripEndIndex > -1) {
				// total csv trips counter (increase only if column is valid i.e at least with one time).
				totalCSVTrips++;	
			}
			
			routeId = getGTFSRouteIdFromRouteShortName(routeShortName);

			if (matrix[5][currentCol] != null && !matrix[5][currentCol].isEmpty()) {
				String lineInfo = HtmlToText.htmlToPlainText(matrix[5][currentCol]);
				if (lineInfo.contains("Linea")) {
					String pdfRouteId = matrix[5][currentCol].substring(matrix[5][currentCol].lastIndexOf('a') + 1);
					pdfRouteId = pdfRouteId.replace("\"", "");
					// check if xx/ routeId exist, else look for xx routeId.
					routeId = getGTFSRouteIdFromRouteShortName(pdfRouteId);
					if (routeId.isEmpty() && pdfRouteId.indexOf("/") != -1) {
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
			
			// (remove)
			if (mergedRoute && !agencyId.equalsIgnoreCase("17")) {
				boolean supportedRoute = false;
				String fileName = outputfileName.substring(0, outputfileName.lastIndexOf(outputPattern + "-annotated.csv") - 1);
				for (FileRouteAgencyModel agencyModel: fileRouteModel.getAgencies()) {
					if (agencyModel.getAgencyId().equalsIgnoreCase(agencyId)) {
						if (agencyModel.getFileRouteMappings().containsKey(fileName)) {
							String[] supportedRouteIds = agencyModel.getFileRouteMappings().get(fileName).split(",");
							for (String keyRouteId: supportedRouteIds) {
								if (keyRouteId.equalsIgnoreCase(routeId)) {
									supportedRoute = true;
									break;
								}
							}
						}
					}
				}
				if (!supportedRoute) {
					ignoredTrips++;
					continue;
				}
			}
			

			// check for unaligned routes.
			boolean isExUrbanUnalignedRoute = false;
			if (routeId.isEmpty() && unalignedRoutesMap.containsKey(routeShortName)) {
				routeId = getGTFSRouteIdFromRouteShortName(unalignedRoutesMap.get(routeShortName).get(0));
				isExUrbanUnalignedRoute = true;
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

					if (verbose) System.out.println("checking column: " + matrix[startRow][currentCol] + " - routeId " + routeId
							+ "[" + startTime + "-" + endTime + "]");

					List<String> tripsForRoute = routeTripsMap.get(routeId);

					if (tripsForRoute.isEmpty()) {
						if (err) System.err.println("no route found");
						columnNotes.add(ROUTE_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

					/** MATCH STEP 1: first and last time matches. **/
					List<String> matchingTripId = new ArrayList<String>();
					String foundTripId = null;

					for (String tripId : tripsForRoute) {
						List<String[]> stopTimes = tripStopsTimesMap.get(tripId);

						if (stopTimes.get(0)[2].contains(startTime)
								&& stopTimes.get(stopTimes.size() - 1)[2].contains(endTime)) {

							if (!matchingTripId.contains(tripId)) {
								matchingTripId.add(tripId);
							}
						}
					}
					
					
					/** MATCH STEP 2: remove old calendar dates. **/
					if (matchingTripId.size() > 1) {
						List<String> copyOfSWTripIds = new ArrayList<String>();
						copyOfSWTripIds.addAll(matchingTripId);

						for (String matchId : copyOfSWTripIds) {

							String serviceId = tripServiceIdMap.get(matchId);

							for (String ignoreServiceId : ignoreServiceIdPattern) {
								if (serviceId.endsWith(ignoreServiceId)) {
									matchingTripId.remove(matchId);
								}
							}
						}
					}
					
					
					/** MATCH STEP 3: validate service calendar(scolastici/non/normal). **/
					// validate service calendar.
					List<String> tempTripIds2 = new ArrayList<String>();
					tempTripIds2.addAll(matchingTripId);
					
					if (tempTripIds2 != null && !tempTripIds2.isEmpty()) {

						for (String matchId : tempTripIds2) {

							// read frequency info.
							String pdfFreqString = "";
							if (matrix[4][currentCol] != null && !matrix[4][currentCol].isEmpty()) {
								pdfFreqString = matrix[4][currentCol].replaceAll("\\s+", " ").toLowerCase();
							}
							if (matrix[3][1] != null && !matrix[3][1].isEmpty()
									&& (pdfFreqString == null | pdfFreqString.isEmpty())) { // read pdf orario type.
								pdfFreqString = matrix[3][1].replaceAll("\\s+", " ").toLowerCase();
							}

							pdfFreqString = pdfFreqString.replace("\"", "");

							if (!frequencyString.contains(pdfFreqString)) {
								frequencyString.add(pdfFreqString);
							}
							
							if (pdfFreqString != null && !pdfFreqString.isEmpty()
									&& pdfFreqStringServiceIdMap.containsKey(pdfFreqString)) {
								
								String servicIdMapIdentifier = pdfFreqStringServiceIdMap.get(pdfFreqString);
								
								String gtfsServiceId = tripServiceIdMap.get(matchId);
								
								//check if it is 'scolastici/non' service.
								if (serviceExceptionType2Dates.containsKey(pdfFreqString)
										| serviceExceptionType1Dates.containsKey(pdfFreqString)) {
									
									// match any exception type 1 day.
									List<String> dayOn = serviceIdExcepType1.get(gtfsServiceId);
									List<String> tempET1 = new ArrayList<String>();
									
									if (serviceExceptionType1Dates.get(pdfFreqString) != null
											&& dayOn != null && !dayOn.isEmpty()) {
										tempET1.addAll(dayOn);
										tempET1.retainAll(serviceExceptionType1Dates.get(pdfFreqString));
									}
									
									// match any exception type 2 day.
									List<String> dayOff = serviceIdExcepType2.get(gtfsServiceId);
									List<String> tempET2 = new ArrayList<String>();
									
									if (serviceExceptionType2Dates.get(pdfFreqString) != null
											&& dayOff != null && !dayOff.isEmpty()) {
										tempET2.addAll(dayOff);
										tempET2.retainAll(serviceExceptionType2Dates.get(pdfFreqString));	
									}
									
									if (tempET1.size() < 1 && tempET2.size() < 1) {
										matchingTripId.remove(matchId);
										continue;
									}
								}
								
								List<String> serviceIds = serviceIdMapping.get(servicIdMapIdentifier);
								if (!serviceIds.contains(gtfsServiceId)) {
									matchingTripId.remove(matchId);
									continue;
								}
							}
						}
					}
					
					/** MATCH STEP 4: search for each stop of pdf in gtfs.and validate service calendar(scolastici/non/normal). **/
					if (matchingTripId == null || matchingTripId.isEmpty()) {
						List<String> tripId = partialTripMatchAlgo(matrix, currentCol, startRow, routeId, isExUrbanUnalignedRoute);
						if (tripId != null && !tripId.isEmpty()) {
							matchingTripId.addAll(tripId);
						} else { // EX-URBAN:  if this route is covered in another route, check for it.
							if (unalignedRoutesMap.containsKey(routeShortName) && agencyId.equalsIgnoreCase("17")) {
								isExUrbanUnalignedRoute = true;
								for (String otherRouteTripId: unalignedRoutesMap.get(routeShortName)) {
									routeId = getGTFSRouteIdFromRouteShortName(otherRouteTripId);
									List<String> otherRoutesTripId = partialTripMatchAlgo(matrix, currentCol, startRow, routeId, isExUrbanUnalignedRoute);
									if (otherRoutesTripId != null && !otherRoutesTripId.isEmpty()) {
										for (String foundOtherTripId: otherRoutesTripId) {
											if (!matchingTripId.contains(foundOtherTripId)) {
												matchingTripId.add(foundOtherTripId);		
											}
										}
									}
								}
							}
						}
//						else { //rerun with 90% match.
//							tripId = partialTripMatchByPercentAlgo(matrix, currentCol, startRow, routeId, 90);
//							if (tripId != null && !tripId.isEmpty()) {
//								matchingTripId.add(tripId);
//							}
//						}
					}

					// check trains.
//					if (matchingTripId == null || matchingTripId.isEmpty() && agencyId.equalsIgnoreCase("17")) {
//						// algorithm to check trip in other route.
//						String tripId = checkTrainTrips(matrix, currentCol, startRow);
//						if (tripId != null && !tripId.isEmpty())
//							isExUrbanUnalignedRoute = true;
//							matchingTripId.add(tripId);
//					}

					/** MATCH STEP 5: filter by hit count of pdf (stop,time) with gtfs (stop,time). **/
					if (matchingTripId.size() > 1) {

						List<String> secondCopyTripIds = new ArrayList<String>();
						secondCopyTripIds.addAll(matchingTripId);

						String prevTripId = secondCopyTripIds.get(0);
						List<String[]> stopTimes = tripStopsTimesMap.get(prevTripId);
						int prevCount = getHitCounts(stopTimes, pdfStopList, matrix, currentCol);

						for (int i = 1; i < secondCopyTripIds.size(); i++) {
							String currentTripId = secondCopyTripIds.get(i);
							List<String[]> stopTimesTemp = tripStopsTimesMap.get(currentTripId);
							int currentCount = getHitCounts(stopTimesTemp, pdfStopList, matrix, currentCol);

							if (currentCount < prevCount) {
								matchingTripId.remove(currentTripId);
							} else {
								matchingTripId.remove(prevTripId);
								prevCount = currentCount;
								prevTripId = currentTripId;
							}
						}

					}
					
					/** MATCH STEP 6: deep match. **/
					for (String matchId : matchingTripId) {
						List<String[]> stopTimes = tripStopsTimesMap.get(matchId);
						// algorithm to check trip in other route.
						if (deepMatchTrips(matrix, currentCol, tripStartIndex, tripEndIndex, stopTimes)) {
							foundTripId = matchId;
							if (isExUrbanUnalignedRoute) {
								unAlignedTripIds.add(foundTripId);
							}
						}
					}
					
					/** POST MATCH STEP 1: prepare stops. **/
					if (foundTripId != null && !foundTripId.isEmpty()) {

						successMatch++;

						if (!matchedTripIds.contains(foundTripId)) {
							matchedTripIds.add(foundTripId);
							//	successMatch++;
						}

						columnTripIdMap.put(currentCol, matchingTripId);

						if (mergedRoute && columnGTFSRSName.containsKey(currentCol)) {
							columnNotes.add(GTFS_RS_NAME + "=" + columnGTFSRSName.get(currentCol).trim());
						}

						List<String[]> stoptimeseq = tripStopsTimesMap.get(matchingTripId.get(0));

						boolean[] stopEntered = new boolean[stopList.size()];
						// RUN FIRST FOR DEPARTURE TIMES.
						for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

							String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "")
									.toLowerCase();
							boolean found = false;
							
							for (int i = 0; i < pdfStopList.size(); i++) {
								boolean appendDepartureString = false;
								// pdf sequence = i + numOfHeaders;
								String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
								pdfStopName = pdfStopName.replaceAll("\"", "");
//								pdfStopName = pdfStopName.replace(" (", "-");
//								pdfStopName = pdfStopName.replace(")", "");

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
										&& stoptimeseq.get(gtfsSeq)[2].contains(pdfTime)) {
									
									// approach to find stops(that can be cyclic in list) prior to logic before
									int stopIndex = -1;
									for (int s = 0; s < stopList.size(); s++) {
										String stopNameInList = stopList.get(s).replace("\"", "");
										stopNameInList = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
										if (stopNameInList.contains(exUrbanDepartureSymbol)) {
											stopNameInList = stopNameInList.replace(exUrbanDepartureSymbol, "").trim();
										}
										if (stopNameInList.equalsIgnoreCase(gtfsStopName) && !stopEntered[i]) {
											stopIndex = i;
											stopEntered[i] = true;
											break;
										}
									}
									if (stopIndex != -1) {
										String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]);
										if (appendDepartureString) {
											stopNameInList = stopNameInList + exUrbanDepartureSymbol;
											appendDepartureString = false;
										}
										stopList.set(i, stopNameInList);	
									}

//									if (stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
//										String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]);
//										if (appendDepartureString) {
//											stopNameInList = stopNameInList + exUrbanDepartureSymbol;
//											appendDepartureString = false;
//										}
//										stopList.set(i, stopNameInList);	
//									}
									
									found = true;
									// if (verbose) System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
									break;
								}
							}

							if (!found && !mergedRoute && !isExUrbanUnalignedRoute) { // && stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1
								anamolies = anamolyMap.get(matchingTripId.get(0) + "$" + currentCol);
								if (anamolies == null) {
									anamolies = new ArrayList<Integer>();
									anamolyMap.put(matchingTripId.get(0) + "$" + currentCol, anamolies);
								}
								if (!anamolies.contains(gtfsSeq)) {
									anamolies.add(gtfsSeq); // adding sequence number.	
								}
									
								if (err) System.err.println( "anamoly - " +  stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
							}
						}

						// run again for arrival.
						for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

							String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "")
									.toLowerCase();
							boolean isArrival = false;
							for (int i = 0; i < pdfStopList.size(); i++) {
								String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
								pdfStopName = pdfStopName.replaceAll("\"", "");
								//pdfStopName = pdfStopName.replace(" (", "-");
								//pdfStopName = pdfStopName.replace(")", "");

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
											&& stoptimeseq.get(gtfsSeq)[1].contains(pdfTime)) {

//										if (stopList.indexOf(stopsMap.get(stoptimeseq.get(gtfsSeq)[3])) == -1) {
//											String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3])
//													+ exUrbanArrivalSymbol;
//											stopList.set(i, stopNameInList);
//										}
										// approach to find stops(that can be cyclic in list) prior to logic before
										int stopIndex = -1;
										for (int s = 0; s < stopList.size(); s++) {
											String stopNameInList = stopList.get(s).replace("\"", "");
											stopNameInList = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
											if (stopNameInList.contains(exUrbanArrivalSymbol)) {
												stopNameInList = stopNameInList.replace(exUrbanArrivalSymbol, "")
														.trim();
											}
											if (stopNameInList.equalsIgnoreCase(gtfsStopName) && !stopEntered[i]) {
												stopIndex = i;
												stopEntered[i] = true;
												break;
											}
										}
										if (stopIndex != -1) {
											String stopNameInList = stopsMap.get(stoptimeseq.get(gtfsSeq)[3])
													+ exUrbanArrivalSymbol;
											stopList.set(i, stopNameInList);	
										}
										
										isArrival = false;
										// if (verbose) System.out.println( i + " - " + stopsMap.get(stoptimeseq.get(gtfsSeq)[3]) + " - " + stoptimeseq.get(gtfsSeq)[3] );
										break;
									}
								}
							}

						}

					} else {
						if (err) System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
						columnNotes.add(TRIP_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

				} else {
					if (err) System.err.println("\n\n\\n--- perhaps no time defined in pdf ---");
//					failedMatch++;
				}

			} else {
				if (err) System.err.println("\n\n\n\n\n----- no route found ----" + matrix[startRow][currentCol]);
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
			if (err) System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
			stopList.set(i, pdfStopName.toLowerCase());

		}
		
		List<String> handledAnomalyStops = new ArrayList<String>();
		// adding anamolies.
		for (String tripId$Col : anamolyMap.keySet()) {

			String[] tripIdColArray = tripId$Col.split("\\$");
			
			String tripId = tripIdColArray[0];
			int columnNo = Integer.valueOf(tripIdColArray[1]);
			
			List<Integer> anamoliesList = anamolyMap.get(tripId$Col);
			List<String[]> stoptimeseq = tripStopsTimesMap.get(tripId);

			for (int anamoly : anamoliesList) {
				String stopId = stoptimeseq.get(anamoly)[3];
				String stopName = stopsMap.get(stopId).toLowerCase();

				if (stopList.indexOf(stopName + exUrbanArrivalSymbol) != -1
						|| stopList.indexOf(stopName + exUrbanDepartureSymbol) != -1) {
					continue;
				}

				
				if (verbose) System.out.println(tripId + "- trying to add stop " + stopName + ":" + stoptimeseq.get(anamoly)[3]);
				String stopNameBefore = null;
				String stopIdBefore = null;
				String stopBeforeDepTime = null;
				String stopBeforeArrTime = null;
				int stopBeforeRowNoInList = -1;

				boolean isArrival = false;
				boolean isAnomalyTime = false;
				boolean isDepartureTime = false;
				for (int a = anamoly - 1; a > -1; a--) {
					stopIdBefore = stoptimeseq.get(a)[3];
					stopNameBefore = stopsMap.get(stopIdBefore).toLowerCase();
					stopBeforeDepTime = stoptimeseq.get(a)[2].substring(0, stoptimeseq.get(a)[2].lastIndexOf(":")).trim();
					stopBeforeArrTime = stoptimeseq.get(a)[1].substring(0, stoptimeseq.get(a)[1].lastIndexOf(":")).trim();
					if (verbose) {
						System.out.println("checking after stop: " + stopNameBefore + " - " + stoptimeseq.get(a)[3]);
					}
					
					if (stopNameBefore != null && !stopNameBefore.isEmpty()) {

//						stopBeforeRowNoInList = stopList.indexOf(stopNameBefore); // if pdf stop.
						
						List<Integer> indexs =new ArrayList<Integer>();
						for (int p = 0; p < stopList.size(); p++) {
						    if(stopList.get(p).equalsIgnoreCase(stopNameBefore)) {
						        indexs.add(p);
						    }
						}
						
						for (int p = 0; p < stopList.size(); p++) {
							    if(stopList.get(p).equalsIgnoreCase(stopNameBefore + "$" + stopIdBefore)) {
							        indexs.add(p);
							        isAnomalyTime = true;
							        
							    }
							}			
						
						if (indexs.isEmpty()) {
							for (int p = 0; p < stopList.size(); p++) {
							    if(stopList.get(p).equalsIgnoreCase(stopNameBefore + exUrbanDepartureSymbol)) {
							        indexs.add(p);
							        isDepartureTime = true;
							    }
							}		
						}
						
						if (indexs.isEmpty()) {
							for (int p = 0; p < stopList.size(); p++) {
							    if(stopList.get(p).equalsIgnoreCase(stopNameBefore + exUrbanArrivalSymbol)) {
							        indexs.add(p);
							        isArrival = true;
							    }
							}	
						}
						
						
						for (int matchIndex: indexs) {
							if (inputPdfTimes.get(matchIndex) != null) {
								List<String> stopTimesInListColumns = inputPdfTimes.get(matchIndex);
								if (stopTimesInListColumns != null && !stopTimesInListColumns.isEmpty()
										&& (stopTimesInListColumns.size() > (columnNo -1))
										&& stopTimesInListColumns.get(columnNo-1).equalsIgnoreCase(stopBeforeDepTime)) {
									stopBeforeRowNoInList = matchIndex;
									break;
								}
							}
						}
						
						if (stopBeforeRowNoInList != -1) {
							break;
						}
//						stopBeforeRowNoInList = stopList.indexOf(stopNameBefore);
//						if (stopBeforeRowNoInList == -1) { 
//							stopBeforeRowNoInList = stopList.indexOf(stopNameBefore + "$" + stopIdBefore); // if recently added anomaly stop
//							isAnomalyTime = true;
//							
//						}
//
//						if (stopBeforeRowNoInList == -1) {
//							stopBeforeRowNoInList = stopList.indexOf(stopNameBefore + exUrbanDepartureSymbol); // check if exist stopName with appended departure string.
//							isDepartureTime = true;
//						}
//
//						if (stopBeforeRowNoInList == -1) {
//							stopBeforeRowNoInList = stopList.indexOf(stopNameBefore + exUrbanArrivalSymbol); // check if exist stopName with appended arrival string.
//							isArrival = true;
//						}

//						if (stopBeforeRowNoInList != -1 && inputPdfTimes.get(stopBeforeRowNoInList) != null) {
//
//							List<String> stopTimesInListColumns = inputPdfTimes.get(stopBeforeRowNoInList);
//							if (stopTimesInListColumns != null && !stopTimesInListColumns.isEmpty()
//									&& (stopTimesInListColumns.size() > (columnNo -1))
//									&& stopTimesInListColumns.get(columnNo - 1).equalsIgnoreCase(stopBeforeDepTime)) {
//								break;
//							}
//						}
					}
				}

				if (stopBeforeRowNoInList != -1 && !isAnomalyTime && !isArrival && !isDepartureTime) { //stopList.lastIndexOf(stopNameBefore) != -1 && !isArrival 
					int insertIndex = stopBeforeRowNoInList + 1;//stopList.lastIndexOf(stopNameBefore) + 1;
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.put(stopId + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim(), -1);
					if (!handledAnomalyStops.contains(stopId)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase() + "$" + stopId);
						pdfStopList.add(insertIndex, "*"+ stopId); // to align with modified stopList.
						List<String> stopTimesInList = new ArrayList<String>();
						for (int i = 0 ; i < noOfCols; i++) {
							stopTimesInList.add(i, "");
						}
						stopTimesInList.add(columnNo - 1, depTime.substring(0, depTime.lastIndexOf(":")).trim()); 
						inputPdfTimes.add(insertIndex, stopTimesInList);
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopId);
					}
				} if (stopBeforeRowNoInList != -1 && isAnomalyTime && !isArrival && !isDepartureTime) { //if (stopList.lastIndexOf(stopNameBefore + "$" + stopIdBefore) != -1 && !isArrival) {
					int insertIndex = stopBeforeRowNoInList + 1;//stopList.lastIndexOf(stopNameBefore + "$" + stopIdBefore) + 1;
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.put(stopId + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim(), -1);
					if (!handledAnomalyStops.contains(stopId)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase() + "$" + stopId);
						pdfStopList.add(insertIndex, "*"+ stopId); // to align with modified stopList.
						List<String> stopTimesInList = new ArrayList<String>();
						for (int i = 0 ; i < noOfCols; i++) {
							stopTimesInList.add(i, "");
						}
						stopTimesInList.add(columnNo - 1, depTime.substring(0, depTime.lastIndexOf(":")).trim()); 
						inputPdfTimes.add(insertIndex, stopTimesInList);
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopId);
					}
				}
				
				else if (stopBeforeRowNoInList != -1 && !isAnomalyTime && isDepartureTime && !isArrival) { //stopList.lastIndexOf(stopNameBefore + exUrbanDepartureSymbol) != -1 && !isArrival
					int insertIndex = stopBeforeRowNoInList + 1;//stopList.lastIndexOf(stopNameBefore + exUrbanDepartureSymbol) + 1;
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.put(stopId + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim(), -1);
					if (!handledAnomalyStops.contains(stopId)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase() + "$" + stopId);
						pdfStopList.add(insertIndex, "*" + stopId); // to align with modified stopList.
						List<String> stopTimesInList = new ArrayList<String>();
						for (int i = 0 ; i < noOfCols; i++) {
							stopTimesInList.add(i, "");
						}
						stopTimesInList.add(columnNo - 1, depTime.substring(0, depTime.lastIndexOf(":")).trim());
						inputPdfTimes.add(insertIndex, stopTimesInList);
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopId);
					}
				} else if (stopBeforeRowNoInList != -1 && !isAnomalyTime && !isDepartureTime && isArrival) { //stopList.lastIndexOf(stopNameBefore + exUrbanArrivalSymbol) != -1 && isArrival
					int insertIndex = stopBeforeRowNoInList + 1;//stopList.lastIndexOf(stopNameBefore + exUrbanArrivalSymbol) + 1;
					String depTime = stoptimeseq.get(anamoly)[2];
					anomalyStopIds.put(stopId + "_" + depTime.substring(0, depTime.lastIndexOf(":")).trim(), -1);
					if (!handledAnomalyStops.contains(stopId)) {
						stopList.add(insertIndex, stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase() + "$" + stopId);
						pdfStopList.add(insertIndex, "*" + stopId); // to align with modified stopList.
						List<String> stopTimesInList = new ArrayList<String>();
						for (int i = 0 ; i < noOfCols; i++) {
							stopTimesInList.add(i, "");
						}
						stopTimesInList.add(columnNo - 1, depTime.substring(0, depTime.lastIndexOf(":")).trim());
						inputPdfTimes.add(insertIndex, stopTimesInList);
						stopIdsMap.put(stopsMap.get(stoptimeseq.get(anamoly)[3]).toLowerCase(),
								stoptimeseq.get(anamoly)[3]);
						handledAnomalyStops.add(stopId);
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
//				if (err) System.err.println("refactoring stopName: " + pdfStopName + " " + stopsMap.containsValue(pdfStopName));
////				if (!stopsFinal.contains(stop.toLowerCase()))
//				stopsFinal.add(pdfStopName.toLowerCase());
////			}
//		}

		return stopList;
//		return stopsFinal;
		
	}

	private int getHitCounts(List<String[]> stoptimeseq, List<String> pdfStopList, String[][] matrix, int currentCol) {
		int hitCounts = 0;

		// RUN FIRST FOR DEPARTURE TIMES.
		for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

			String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "").toLowerCase();
		
			for (int i = 0; i < pdfStopList.size(); i++) {
				// pdf sequence = i + numOfHeaders;
				String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
				pdfStopName = pdfStopName.replaceAll("\"", "");
				//				pdfStopName = pdfStopName.replace(" (", "-");
				//				pdfStopName = pdfStopName.replace(")", "");

				if (pdfStopName.contains(exUrbanArrivalSymbol)) {
					continue;

				}

				if (pdfStopName.contains(exUrbanDepartureSymbol)) {
					pdfStopName = pdfStopName.replace(exUrbanDepartureSymbol, "").trim();
				}

				String pdfTime = "";
				if (matrix[i + numOfHeaders][currentCol] != null && !matrix[i + numOfHeaders][currentCol].contains("|")
						&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {
					pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";

					int pdfTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));

					if (pdfTimeHour > 24) {
						pdfTimeHour = pdfTimeHour - 24;
					}

					pdfTime = formatter.format(pdfTimeHour) + pdfTime.substring(pdfTime.indexOf(":")).trim();
				}
				stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(), stoptimeseq.get(gtfsSeq)[3]);

				if (pdfStopName.equalsIgnoreCase(gtfsStopName) && stoptimeseq.get(gtfsSeq)[2].contains(pdfTime) && !pdfTime.isEmpty()) {

					hitCounts++;
					break;
				}
			}

		}
		
		// run again for arrival.
		for (int gtfsSeq = 0; gtfsSeq < stoptimeseq.size(); gtfsSeq++) {

			String gtfsStopName = stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).replaceAll("\"", "").toLowerCase();
			boolean isArrival = false;
			for (int i = 0; i < pdfStopList.size(); i++) {
				String pdfStopName = pdfStopList.get(i).replaceAll("\\s+", " ").toLowerCase();
				pdfStopName = pdfStopName.replaceAll("\"", "");
				//pdfStopName = pdfStopName.replace(" (", "-");
				//pdfStopName = pdfStopName.replace(")", "");

				if (pdfStopName.contains(exUrbanArrivalSymbol)) {
					pdfStopName = pdfStopName.replace(exUrbanArrivalSymbol, "").trim();
					isArrival = true;

				} else {
					continue;
				}

				String pdfTime = "";
				if (matrix[i + numOfHeaders][currentCol] != null && !matrix[i + numOfHeaders][currentCol].contains("|")
						&& !(matrix[i + numOfHeaders][currentCol].isEmpty())) {
					pdfTime = matrix[i + numOfHeaders][currentCol].replace(".", ":") + ":00";

					int pdfTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));

					if (pdfTimeHour > 24) {
						pdfTimeHour = pdfTimeHour - 24;
					}
					pdfTime = formatter.format(pdfTimeHour) + pdfTime.substring(pdfTime.indexOf(":")).trim();
				}
				stopIdsMap.put(stopsMap.get(stoptimeseq.get(gtfsSeq)[3]).toLowerCase(), stoptimeseq.get(gtfsSeq)[3]);

				if (isArrival) {
					if (pdfStopName.equalsIgnoreCase(gtfsStopName) && stoptimeseq.get(gtfsSeq)[1].contains(pdfTime) && pdfTime.isEmpty()) {
						isArrival = false;
						hitCounts++;
						break;
					}
				}
			}

		}

		return hitCounts;
	}

	private String checkTrainTrips(String[][] matrix, int colInPdf, int startRow) {
    	String trainTripId = "";

		if (verbose) System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

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

							if (verbose) System.out.println("check all trips for time " + matrix[t][colInPdf]);
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
						if (verbose) System.out.println("found partial matched trip Id " + matchingTripId.get(0));
						trainTripId = matchingTripId.get(0);
					} else {
						if (err) System.err.println("anamoly- mutliple trips detected");
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

		if (verbose) System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

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

						if (verbose) System.out.println("check all trips for time " + matrix[t][colInPdf]);
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
					if (verbose) System.out.println("found partial matched trip Id " + matchingTripId.get(0));
					partialTripId = matchingTripId.get(0);
				} else {
					if (err) System.err.println("anamoly- mutliple trips detected");
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

		if (stopTimes != null && !stopTimes.isEmpty()) {
			
			for (i = tripStartIndex; i <= tripEndIndex; i++) {

				//	boolean isArrivalTime = false;

				if (matrix[i][currentCol] == null || matrix[i][currentCol].isEmpty()
						|| matrix[i][currentCol].contains("|") || matrix[i][currentCol].contains("-")) {
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

				timeToCheck = formatter.format(timeToCheckHour)
						+ timeToCheck.substring(timeToCheck.indexOf(":")).trim();

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
					if (err)
						System.err.println("probably misaligned GTFS time, compare tripId: " + stopTimes.get(0)[0]
								+ " times with PDF");
					return false;
				}
			}

			if (i == tripEndIndex + 1) {
				return true;
			}
		}

		return false;

	}
	
	private RouteModel readRouteModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("tn-routemodel.json"),
				RouteModel.class);
	}
	
	private FileRouteModel readFileRouteConfigurationModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("file-route.json"),
				FileRouteModel.class);
	}

	public static void main(String[] args) throws Exception {
		AnnotatedTTGenerator timeTableGenerator = new AnnotatedTTGenerator();
		File folder = new File(pathToInput);

		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory() | fileEntry.getName().contains(".json") | fileEntry.getName().contains(".zip")) {
				continue;
			} else {
				if (verbose) System.out.println("Annotation in process for ->  " + fileEntry.getName());
				timeTableGenerator.processFiles(pathToOutput, agencyId, pathToInput + fileEntry.getName());
			}
		}

//		timeTableGenerator.processFiles(pathToOutput, "12", pathToInput + "05A-Feriale.csv");
//		timeTableGenerator.processFiles(pathToOutput, "16", pathToInput + "I-01A-Feriale.csv");
//		timeTableGenerator.processFiles(pathToOutput, "16", pathToInput + "P-07R-Feriale.csv");
//		timeTableGenerator.processFiles(pathToOutput, "12", pathToInput + "07A-Feriale.csv");
		
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "503ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "401INA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "231A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "406A-R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "119ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "646R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "122ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "461R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "503ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "646A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "332A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "511ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "203A-R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "131ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "104A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "206R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "511ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "111A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "630R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "461A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "403R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "120ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "231R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "111R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "503ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "463A-A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "307R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "501R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "205A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "321A-R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "645R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "204A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "332A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "403A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "204R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "301R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "306R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "462R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "335A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "206A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "307A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "630A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "120ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "506ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "467A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "332R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "501A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "642R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "405R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "321A-A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "643A-R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "405A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "122ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "306A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "101ESA.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "201R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "406A-A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "401INR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "464R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "642A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "201A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "463A-R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "104R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "103A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "462A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "645A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "620R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "303A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "310ESA-A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "467R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "312A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "620A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "464A.csv");
		
		//fix stops.
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "111R.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "131ESR.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "315A.csv");
//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput + "307A.csv");

		timeTableGenerator.printStats();

		// hard fix mode.
		if (deepMode) {

			timeTableGenerator.deepFixMode();

			timeTableGenerator.destroy();

			timeTableGenerator.printStats();
		}
		
//		for (String servizio: servizioString) {
//			System.err.println(servizio);
//		}
		
//		for (String fixedFileName : fixedOrderList) {
//			System.err.println(fixedFileName);
//		}
		
		for (String freq: frequencyString) {
			System.err.println(freq);
		}

	}

}
