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

public class CustomTTGenerator {

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
	private static boolean verbose = true;
	// err.
	private static boolean err = false;
	// input GTFS.
	private static final String pathToGTFS = "src/test/resources/gtfs/17/";
	// output folder.
	private static final String pathToOutput = "src/test/resources/annotatedtimetable/17'/";
	// input folder.
	private static final String pathToInput = "src/test/resources/inputtimetable/17'/";
	// agencyIds (12,16,17)
	private static final String agencyId = "17";
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

	String mismatchColIds = "";

	private List<String> ignoreServiceIdPattern = new ArrayList<String>() {
		{
			// urban.
			add("2015062620150909");
			// ex-urban(ignore winter).
			// add("20160624");
			// add("20160329");
			// add("20160106");
			// add("20160607");
			// add("20151031");
			// add("20151222");
			// ex-urban (ignore summer.)
			// add("20150909");
			// add("20150831");

		}
	};

	// urban.
	private String outputPattern = "2017091020180622"; // "2017062420170909"
	// ex-urban.
	// private String outputPattern = "2015091020160624";
	// //2015091020160624,2015062620150909

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

	private static final Map<String, String> pdfFreqStringServiceIdMap = new HashMap<String, String>();
	{
		pdfFreqStringServiceIdMap.put("solo nei giorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale da lunedÃ¬ a venerdÃ¬", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("scolastica da lunedÃ¬ a venerdÃ¬", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("feriale solo il sabato", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("Scolastica solo il Sabato", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("solo nei giorni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei giorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale escluso sabato", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("solo al sabato feriale", CALENDAR_SOLOSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("feriale escluso sab", CALENDAR_LUNVEN);
		pdfFreqStringServiceIdMap.put("solo al mercoledÃ¬ feriale", CALENDAR_SOLMERCOLEDI);
		pdfFreqStringServiceIdMap.put("solo nei ggiorni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei gg. festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("solo al giovedÃ¬ feriale", CALENDAR_SOLGIOV);
		pdfFreqStringServiceIdMap.put("solo nei girni feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("solo nei ggiorni festivi", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("orario festivo", CALENDAR_FESTIVO);
		pdfFreqStringServiceIdMap.put("orario feriali", CALENDAR_LUNSAB);
		pdfFreqStringServiceIdMap.put("orario feriale", CALENDAR_LUNSAB);
		// "Postfestivo"
	}

	List<String> deleteList = new ArrayList<String>();
	private static List<String> emptyFreqCSVS = new ArrayList<String>();

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

	private static List<String> servizioString = new ArrayList<String>();

	private static Map<Integer, String> columnServiceStringMap = new HashMap<Integer, String>();

	private static List<String> allServiceIds = new ArrayList<String>();

	private static FileRouteModel fileRouteModel;

	public CustomTTGenerator() throws IOException {
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
			boolean isRitorno = false;
			String outputName = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("."));

			if (outputName.endsWith("R")) {
				isRitorno = true;
			}
			// ex-urban.
			if (agencyId.equalsIgnoreCase("17")) {
				outputName = outputName.replace("-", "_");
			}
			outputName = outputName + "-" + outputPattern + "-annotated.csv";
			File file = new File(filename);
			List<String> lines = Files.asCharSource(file, Charsets.UTF_8).readLines();
			annotated.addAll(convertLines(lines, outputName, isRitorno));
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
		columnServiceStringMap.clear();

	}

	private List<String> convertLines(List<String> lines, String outputFileName, boolean isRitorno) throws Exception {

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

			// String[] colValues = table[i][0].split(";");
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
		routeShortName = matrixFiltered[0][1].replaceAll("\"", "");

		// annotation process.
		int noOfOutputCols = maxNumberOfCols + 1;

		String[][] output;
		output = processMatrix(matrixFiltered, noOfOutputCols, outputFileName, isRitorno);

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

	private String[][] processMatrix(String[][] matrix, int noOfOutputCols, String outputFileName, boolean isRitorno) {

		// create list of stops taking in to consideration GTFS data.
		List<String> stops = new ArrayList<String>();

		// prepare list of List<String> for input matrix.
		List<List<String>> inputCSVTimes = createListOfList(matrix, numOfHeaders, noOfOutputCols);

		stops = processStops(matrix, numOfHeaders, noOfOutputCols - 1, inputCSVTimes, outputFileName, isRitorno);

		int noOfOutputRows = (stops.size() + 5);
		String[][] output = new String[noOfOutputRows][noOfOutputCols];

		// stops column.
		output[0][0] = "gtfs trip_id;";
		output[1][0] = "smartplanner route_id;";
		output[2][0] = "service_id;";
		output[3][0] = "stops;stop_id";

		for (int i = 0; i < stops.size(); i++) {
			output[i + 4][0] = stops.get(i) + ";";
		}

		for (int j = 1; j < noOfOutputCols - 1; j++) {

			if (columnTripIdMap.containsKey(j)) {
				boolean traversed[] = new boolean[stops.size()];
				/** logic for handling cyclic trips for e.g. A_C festivo. **/
				int foundIndex = -1;
				for (int i = 0; i < stops.size(); i++) {

					// logic for taking stopName of final List.
					String stopName = stops.get(i).toLowerCase(); // .replace("\"",
																	// "");
					if (stopName.contains(exUrbanDepartureSymbol)) {
						// String[] stopMatchPart =
						// stopName.split(exUrbanDepartureSymbol);
						stopName = stopName.substring(0, stopName.indexOf(exUrbanDepartureSymbol));
					}

					if (stopName.contains(exUrbanArrivalSymbol)) {
						stopName = stopName.substring(0, stopName.indexOf(exUrbanArrivalSymbol));
					}

					List<String> inputCsvTimeList = null;
					if (inputCSVTimes.get(i) != null) {
						inputCsvTimeList = inputCSVTimes.get(i);
					}

					String inputPdfTime = "";
					foundIndex = i;
					if (inputCsvTimeList != null && !inputCsvTimeList.isEmpty() && inputCsvTimeList.size() > (j - 1)) {
						inputPdfTime = inputCsvTimeList.get(j - 1);

						output[foundIndex + 4][j] = inputPdfTime;

						String outputStopName = stops.get(i);
						String stopId = stopName;

						stopId = stopId.replace(")", "");
						if (stopId.endsWith(" ")) {
							stopId = stopId.substring(0, stopId.lastIndexOf(" "));
						}
						stopId = stopId.replace("(", "_");
						stopId = stopId.replace(" ", "_");

						output[foundIndex + 4][0] = outputStopName + ";" + stopId;
					}
				}
			}

			// fill in italic entries.
			boolean[] italicEntered = new boolean[stops.size()];
			for (String italicEntry : columnItalicStopNames.get(j)) {
				String name = italicEntry.substring(0, italicEntry.indexOf("$"));
				String time = italicEntry.substring(italicEntry.indexOf("$") + 1);
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

		if (columnServiceStringMap.size() <= 0) {
			if (!emptyFreqCSVS.contains(outputFileName))
				;
			emptyFreqCSVS.add(outputFileName);
		}

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
		String serviceId = "AD_winter"; // switch between summer (24-06.2017 to 09-09-2017) and winter(10-09-2017 - 230602017)

		if (columnServiceStringMap.get(col) != null && !columnServiceStringMap.get(col).isEmpty()) {
			serviceId = columnServiceStringMap.get(col).replace(" ", "_");
			serviceId = serviceId.replace("Ã¬", "i");
			serviceId = serviceId.replace("?", "i");
			serviceId = serviceId + "_winter"; //switch between summer (24-06.2017 to 09-09-2017) and winter(10-09-2017 - 230602017)
			serviceId = serviceId.replace("__", "_");
			serviceId = serviceId.replace("�", "i");
			serviceId = serviceId.replace("ì", "i");

		}

		if (!allServiceIds.contains(serviceId)) {
			allServiceIds.add(serviceId);
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
				String key = gtfsTripId.substring(0, gtfsTripId.indexOf("-")); // routeId
																				// +
																				// "_"
																				// +
																				// agencyId
																				// +
																				// "_"
																				// +
																				// directionId;
				sayservice.RouteModel.AgencyModel am = routeModel.agency(agencyId);
				if (am.getRouteMappings() != null && am.getRouteMappings().containsKey(key)) {
					cacheRouteId = am.getRouteMappings().get(key);
				} else {
					cacheRouteId = key;
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
			gtfsTripId = gtfsTripId + "_winter"; // ?? use for summer trip ids.
		}

		return gtfsTripId;

	}

	private List<List<String>> createListOfList(String[][] matrix, int numOfHeaders, int noOfCols) {

		List<List<String>> csvList = new ArrayList<List<String>>();

		for (int i = numOfHeaders; i < matrix.length; i++) {

			List<String> temp = new ArrayList<String>();
			for (int j = 1; j < noOfCols; j++) {
				if (matrix[i][j] != null && !matrix[i][j].contains("|") && !matrix[i][j].contains("|")
						&& !matrix[i][j].isEmpty()) {
					String pdfTime = matrix[i][j].replace(".", ":");
					System.err.println(pdfTime);
					int startTimeHour = Integer.valueOf(pdfTime.substring(0, pdfTime.indexOf(":")));
					String formattedTime = formatter.format(startTimeHour)
							+ pdfTime.substring(pdfTime.indexOf(":")).trim();
					temp.add(formattedTime);
				} else {
					temp.add("");
				}
			}

			csvList.add(i - numOfHeaders, temp);
		}

		return csvList;
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

		if (verbose)
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

				if (stopTimes.get(0)[2].contains(startTime)
						&& stopTimes.get(stopTimes.size() - 1)[2].contains(endTime)) {

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
					if (err)
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
								String stopName = output[i - numOfHeaders + 1][0].substring(0,
										output[i - numOfHeaders + 1][0].indexOf(";"));
								String stopId = "";
								if (output[i - numOfHeaders + 1][0].contains("~")) {
									stopId = output[i - numOfHeaders + 1][0]
											.substring(output[i - numOfHeaders + 1][0].indexOf(";") + 2);
								} else {
									stopId = output[i - numOfHeaders + 1][0]
											.substring(output[i - numOfHeaders + 1][0].indexOf(";") + 1);
								}
								if (!stopId.equalsIgnoreCase(stoptimeseq.get(s)[3] + "_" + agencyId)) {
									if (err)
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
				if (err)
					System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
				annotation = "no trip found";
			}
		} else {
			if (err)
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
	 * 
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

			/**
			 * very important (pdf seems to contain time mapped to departure
			 * time in stoptimes.txt.) stopTimes.get(s)[2] departure time.
			 * stopTimes.get(s)[1] arrival time.
			 **/
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
		// if (verbose) System.out.println(regexQuery.toString());
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
		// HashMap<String, String> stopsMap = new HashMap<String, String>();

		List<String[]> linesTrip = readFileGetLines(tripFile);
		List<String[]> linesST = readFileGetLines(stoptimesTFile);
		List<String[]> stops = readFileGetLines(stopFile);
		List<String[]> calendar = readFileGetLines(calendarFile);
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

		// create mapping for calendar LV,F,AD,SS,LS
		for (String serviceId : calendarEntries.keySet()) {

			for (String ignoreServiceId : ignoreServiceIdPattern) {
				if (serviceId.endsWith(ignoreServiceId)) {
					continue;
				}
			}

			boolean[] b = calendarEntries.get(serviceId);
			List<String> serviceIds = null;
			if (b[0] & b[1] & b[2] & b[3] & b[4] & b[5] & b[6]) { // AD.
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
			} else if (!b[0] & !b[1] & !b[2] & !b[3] & !b[4] & !b[5] & b[6]) { // F
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
			} else if (!b[0] & !b[1] & b[2] & !b[3] & !b[4] & !b[5] & !b[6]) { // SMERC
				serviceIds = serviceIdMapping.get(CALENDAR_SOLMERCOLEDI);
				if (serviceIds == null) {
					serviceIds = new ArrayList<String>();
					serviceIdMapping.put(CALENDAR_SOLMERCOLEDI, serviceIds);
				}
				serviceIds.add(serviceId);
			} else if (!b[0] & !b[1] & !b[2] & b[3] & !b[4] & !b[5] & !b[6]) { // SGIOV
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
				if (verbose)
					System.out.println("Error parsing route: " + words[0] + "," + words[1] + "," + words[2]);
			}
		}
		return routeId;
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
					if (verbose)
						System.out.println("fixing column " + Integer.valueOf(noOfcols[c]));
					stepOneOutputMatrix[numOfHeaders][Integer.valueOf(noOfcols[c]) - 1] = annotation;
				}
			}
		}

		return converted;

	}

	private List<String> partialTripMatchAlgo(String[][] matrix, int colInPdf, int startRow, String routeId,
			boolean isUnAlignedRoute) {

		List<String> matchingTripId = new ArrayList<String>();

		if (verbose)
			System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

		if (!isUnAlignedRoute) {

			// validate trip with GTFS.
			boolean[] toBeCheckTimeIndex = new boolean[matrix.length];

			for (int i = startRow; i < matrix.length; i++) {

				if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty()
						&& !matrix[i][colInPdf].contains("|")) {
					if (matrix[i][colInPdf].contains("-")) {
						continue;
					}
					toBeCheckTimeIndex[i] = true;
				}
			}

			if (routeId != null && !routeId.isEmpty()) {

				List<String> tripsForRoute = routeTripsMap.get(routeId);

				if (tripsForRoute.isEmpty()) {
					// partialTripId = "no route found";
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
								// matches arrival or departure time (since pdf
								// contains both entries).
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

				if (matrix[i][colInPdf] != null && !matrix[i][colInPdf].isEmpty()
						&& !matrix[i][colInPdf].contains("|")) {
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
								// matches arrival or departure time (since pdf
								// contains both entries).
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

		List<String> copyOfTripIds = new ArrayList<String>();
		copyOfTripIds.addAll(matchingTripId);

		if (copyOfTripIds != null && !copyOfTripIds.isEmpty()) {

			for (String matchId : copyOfTripIds) {

				// read frequency info.
				if (matrix[4][colInPdf] != null && !matrix[4][colInPdf].isEmpty()) {
					String pdfFreqString = matrix[4][colInPdf].replaceAll("\\s+", " ").toLowerCase();
					pdfFreqString = pdfFreqString.replace("\"", "");
					if (pdfFreqStringServiceIdMap.containsKey(pdfFreqString)) {
						String servicIdMapIdentifier = pdfFreqStringServiceIdMap.get(pdfFreqString);
						List<String> serviceIds = serviceIdMapping.get(servicIdMapIdentifier);
						if (!serviceIds.contains(tripServiceIdMap.get(matchId))) {
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

		if (verbose)
			System.out.println("Processing column starting with time: " + matrix[numOfHeaders][colInPdf]);

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
					if (toBeCheckTimeIndex[t] && matrix[t][colInPdf] != null && !matrix[t][colInPdf].isEmpty()) {
						String timeToCheck = matrix[t][colInPdf].replace(".", ":");
						if (verbose)
							System.out.println("check all trips for time " + matrix[t][colInPdf]);
						for (int s = 0; s < stopTimes.size(); s++) {
							// matches arrival or departure time (since pdf
							// contains both entries).
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
					if (verbose)
						System.out.println("improved situation.... found trip Id " + matchingTripId.get(0));
					annotation = matchingTripId.get(0);
					if (!deepMatchedTripIds.contains(matchingTripId.get(0))) {
						deepMatchedTripIds.add(matchingTripId.get(0));
					}
				} else {
					if (err)
						System.err.println("anamoly- mutliple trips detected");
					for (String tripId : matchingTripId) {
						annotation = annotation + "-" + tripId;

					}
				}
			} else {
				if (err)
					System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][colInPdf]);
				annotation = "no trip found";
			}
		} else {
			if (err)
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
			// stats.
			if (routeStats) {
				System.out.println("%%%%%%%%%% RUN STATS %%%%%%%%%%");
				System.out.println("successful matches: " + successMatch);
				System.out.println("failed matches: " + failedMatch);
				System.out.println("success rate: " + (successMatch / (successMatch + failedMatch)) * 100);
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			}

			if (csvStats) {
				System.out.println("%%%%%%%%%% CSV STATS %%%%%%%%%%");
				System.out.println("total csv trips: " + totalCSVTrips);
				System.out.println("csv trips covered by GTFS: " + successMatch);
				System.out.println("csv trips not covered by GTFS: " + failedMatch);
				System.out.println("csv coverage: " + (successMatch / (totalCSVTrips)) * 100);
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

			}
			if (gtfsStats) {
				System.out.println("\n\n\n\n");
				System.out.println("%%%%%%%%%%%%%%% GTFS STATS %%%%%%%%%%%%%%");
				System.out.println("total number of GTFS trips for routes: " + gtfsTripIds.size());
				System.out.println("total number of matched trips for routes: " + matchedTripIds.size());
				System.out.println("coverage(normal) : "
						+ (Double.valueOf(matchedTripIds.size()) / Double.valueOf(gtfsTripIds.size())) * 100);
				if (deepMode) {
					System.out.println("Fixes in deep search mode :" + deepMatchedTripIds.size());
					System.out.println(
							"coverage(deep) : " + (Double.valueOf(matchedTripIds.size() + deepMatchedTripIds.size())
									/ Double.valueOf(gtfsTripIds.size())) * 100);
				}
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				System.out.println("\n\n\n\n");
				List<String> deltaTrips = new ArrayList<String>();
				deltaTrips.addAll(gtfsTripIds);
				deltaTrips.removeAll(deepMatchedTripIds);
				deltaTrips.removeAll(matchedTripIds);
				System.out.println("Trips Delta size :" + deltaTrips.size());
				for (String tripId : deltaTrips) {
					System.out.println(tripId);
				}
			}

		}
	}

	private List<String> processStops(String[][] matrix, int startRow, int noOfCols, List<List<String>> inputPdfTimes,
			String outputfileName, boolean isRitorno) {

		// merged list of stops.
		List<String> stopList = new ArrayList<String>();
		// pdf list of stops.
		List<String> pdfStopList = new ArrayList<String>();
		List<Integer> anamolies = null;

		for (int i = 0; i < (matrix.length - numOfHeaders); i++) {
			String pdfStopName = matrix[i + numOfHeaders][0].trim();
			pdfStopName = pdfStopName.replaceAll("\\s+", " ");
			pdfStopName = pdfStopName.replace(" )", ")");
			pdfStopName = pdfStopName.replace(" (", "(");
			pdfStopName = pdfStopName.replace(". ", ".");
			pdfStopName = pdfStopName.replaceAll("\"", "");
			// }
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

			if (matrix[4][currentCol] != null && !matrix[4][currentCol].isEmpty()) {
				String pdfFreqString = matrix[4][currentCol].replaceAll("\\s+", " ").toLowerCase();
				pdfFreqString = pdfFreqString.replace("\"", "");
				columnServiceStringMap.put(currentCol, pdfFreqString);
			}

			// column italic stopNames.
			List<String> italicStopEntry = new ArrayList<String>();
			columnItalicStopNames.put(currentCol, italicStopEntry);

			int tripStartIndex = -1;
			for (int i = startRow; i < matrix.length; i++) {
				if (matrix[i][currentCol] != null && !matrix[i][currentCol].isEmpty()
						&& !matrix[i][currentCol].contains("|")) {
					if (matrix[i][currentCol].contains("-")) {
						// italics = true;
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
						// italics = true;
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
				// total csv trips counter (increase only if column is valid i.e
				// at least with one time).
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

			// check for unaligned routes.
			// boolean isExUrbanUnalignedRoute = false;
			if (routeId.isEmpty() && unalignedRoutesMap.containsKey(routeShortName)) {
				routeId = getGTFSRouteIdFromRouteShortName(unalignedRoutesMap.get(routeShortName).get(0));
				// isExUrbanUnalignedRoute = true;
			}

			if (routeId != null && !routeId.isEmpty()) {

				if (tripStartIndex > -1 && tripEndIndex > -1) {

					List<String> matchingTripId = new ArrayList<String>();
					String foundTripId = routeId + "_" + agencyId + "_" + (isRitorno ? 1 : 0) + "-" + routeShortName
							+ "_" + (isRitorno ? "ritorno" : "andata") + "_" + currentCol;
					matchingTripId.add(foundTripId);

					// prepare stops list.
					if (foundTripId != null && !foundTripId.isEmpty()) {

						successMatch++;

						if (!matchedTripIds.contains(foundTripId)) {
							matchedTripIds.add(foundTripId);
							// successMatch++;
						}

						columnTripIdMap.put(currentCol, matchingTripId);

					} else {
						if (err)
							System.err.println("\n\n\n\n\n----- no trip found ----" + matrix[startRow][currentCol]);
						columnNotes.add(TRIP_ERROR);
						failedMatch++;
						mismatchColIds = mismatchColIds + (currentCol + 2) + ",";
					}

				} else {
					if (err)
						System.err.println("\n\n\\n--- perhaps no time defined in pdf ---");
					// failedMatch++;
				}

			} else {
				if (err)
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
			// if (err) System.err.println("refactoring stopName: " +
			// pdfStopName + " " + stopsMap.containsValue(pdfStopName));
			stopList.set(i, pdfStopName);
			//
		}

		return stopList;
		// return stopsFinal;

	}

	private RouteModel readRouteModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("tn-routemodel.json"),
				RouteModel.class);
	}

	private FileRouteModel readFileRouteConfigurationModel()
			throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("file-route.json"),
				FileRouteModel.class);
	}

	public static void main(String[] args) throws Exception {
		CustomTTGenerator timeTableGenerator = new CustomTTGenerator();
		File folder = new File(pathToInput);

		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory() | fileEntry.getName().contains(".json")
					| fileEntry.getName().contains(".zip")) {
				continue;
			} else {
				if (verbose)
					System.out.println("Annotation in process for ->  " + fileEntry.getName());
				timeTableGenerator.processFiles(pathToOutput, agencyId, pathToInput + fileEntry.getName());
			}
		}

//		timeTableGenerator.processFiles(pathToOutput, "17", pathToInput +  "336A-R.csv");
		timeTableGenerator.printStats();

		// hard fix mode.
		// if (deepMode) {
		//
		// timeTableGenerator.deepFixMode();
		//
		// timeTableGenerator.destroy();
		//
		// timeTableGenerator.printStats();
		// }

		// for (String servizio: servizioString) {
		// System.err.println(servizio);
		// }

		for (String servizio : allServiceIds) {
			System.err.println(servizio);
		}

		Thread.currentThread().sleep(2000);
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("CSVs without frequency.");
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.err.println(emptyFreqCSVS.toString());

	}

}
