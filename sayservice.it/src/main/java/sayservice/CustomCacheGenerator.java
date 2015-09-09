package sayservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.gdata.util.common.html.HtmlToText;
import com.google.gdata.util.io.base.UnicodeReader;

public class CustomCacheGenerator {
	
	private static String pathToAnnotatedCSVs = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/annotatedtimetable/17'";
//	private static String pathToAnnotatedCSVs = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/annotatedtimetable/16";
//	private static String pathToAnnotatedCSVs = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/annotatedtimetable/17";
	private static final String pathToGTFS = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/gtfs/17/";
//	private static final String pathToGTFS = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/gtfs/16/";
//	private static final String pathToGTFS = "C:/projects/marco/annotated.timetable/sayservice.it/src/test/resources/gtfs/17/";
//	private static String pathToOutput = "C:/tmp/annotated-cache/12";
//	private static String pathToOutput = "C:/tmp/annotated-cache/16";
	private static String pathToOutput = "C:/tmp/annotated-cache/17";
	// agencyIds (12,16,17).
	private static String agencyId = "17";
	private static final String UTF8_BOM = "\uFEFF";
	private static int numOfHeaders = 11;
	private static final List<String> roveretoNBuses = Arrays.asList("N1", "N2", "N3", "N5", "N6");
	private static File tripsTxt = new File(pathToOutput, "trips.txt");
	private static File stopTimesTxt = new File(pathToOutput, "stop_times.txt");
	private static File stopTxt = new File(pathToOutput, "stops.txt");
	private static List<String> cachTrips = new ArrayList<String>();
	private static List<String> cacheStoptimes = new ArrayList<String>();
	private static List<String> stopEntries = new ArrayList<String>();
	private static Map<String, List<String>> tripRouteServiceHeadsignIdMap = new HashMap<String, List<String>>();
	
	private static String[] andataSuffix = new String[] { "A-2015091020160607-annotated.csv", "a-2015091020160607-annotated.csv",
			"A-Feriale-annotated.csv", "a-Feriale-annotated.csv", "A-Festivo-annotated", "a-Festivo-annotated" };
	
	private static String[] ritornoSuffix = new String[] { "R-2015091020160607-annotated.csv", "r-2015091020160607-annotated.csv",
			"R-Feriale-annotated.csv", "r-Feriale-annotated.csv", "R-Festivo-annotated", "r-Festivo-annotated" };	

	private static RouteModel routeModel;
	
	private static Map<String, String> stopIdNameMap = new HashMap<String, String>();
	
	public static void main(String args[]) throws IOException {

		CustomCacheGenerator aCacheGenerator = new CustomCacheGenerator();
		aCacheGenerator.init();

		File folder = new File(pathToAnnotatedCSVs);

		int totalFile = 0;
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory() | fileEntry.getName().contains(".json")
					| fileEntry.getName().contains(".zip")) {
				continue;
			} else {
				System.out.println("Cache generation in process for ->  " + fileEntry.getName());
				aCacheGenerator.processFiles(agencyId, fileEntry.getName());
			}
			totalFile++;
		}

		Files.asCharSink(tripsTxt, Charsets.UTF_8).writeLines(cachTrips);
		Files.asCharSink(stopTimesTxt, Charsets.UTF_8).writeLines(cacheStoptimes);
		Files.asCharSink(stopTxt, Charsets.UTF_8).writeLines(stopEntries);
		System.out.println("Total files processed =  " + totalFile);
	}

	private void processFiles(String agencyId2, String fileName) throws IOException {

		File file = new File(pathToAnnotatedCSVs, fileName);
		List<String> lines = Files.asCharSource(file, Charsets.UTF_8).readLines();
		
		String pdfName = fileName.substring(0, fileName.lastIndexOf("-annotated.csv"));

		List<String> trips = new ArrayList<String>();
		List<String> stoptimes = new ArrayList<String>();
		
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

		// extract GTFS information and structures.
		String routeShortName = matrix[0][2].replaceAll("\"", "");
		String routeDescription = matrix[1][2].replaceAll("\"", "");

//		System.out.println(routeShortName);

		for (int j = 2; j < maxNumberOfCols - 1; j++) {

			String gtfsTripId = matrix[numOfHeaders - 4][j];
			String serviceId = matrix[numOfHeaders - 2][j];
			
			if (gtfsTripId != null) {
			
				if (gtfsTripId.indexOf(",") != -1) {
					String[] tripIds = gtfsTripId.split(",");
					gtfsTripId = tripIds[0];
				}
				
				if (gtfsTripId.startsWith("*")) {
					gtfsTripId = gtfsTripId.substring(1);
				}
				
				if (gtfsTripId.contains("$")) {
					gtfsTripId = gtfsTripId.substring(0, gtfsTripId.indexOf("$"));
				}
				
				gtfsTripId = gtfsTripId.replaceAll("\\s+", "");

			}
			
			String annotation = "";

//			if (tripRouteServiceHeadsignIdMap.containsKey(gtfsTripId)) {

//				List<String> tripInfoGTFS = tripRouteServiceHeadsignIdMap.get(gtfsTripId);

				String directionId = ""; 
				String routeId = ""; //tripInfoGTFS.get(0);
				
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
				
				// identify new routeId
				String key = gtfsTripId.substring(0, gtfsTripId.indexOf("-"));//routeId + "_" + agencyId + "_" + directionId;
				sayservice.RouteModel.AgencyModel am = routeModel.agency(agencyId);
				String cacheRouteId = null;
				
				if (am.getRouteMappings() != null && am.getRouteMappings().containsKey(key)) {
					cacheRouteId = am.getRouteMappings().get(key);
				} else {
					cacheRouteId = key;
				}

				String cacheTripId = gtfsTripId;
				String gtfsRouteId = cacheRouteId.substring(0, cacheRouteId.indexOf("_")); 
				
				
				if (matrix[5][j] != null && !matrix[5][j].isEmpty()) {

					String lineInfo = HtmlToText.htmlToPlainText(matrix[5][j]);
					lineInfo = lineInfo.replaceAll("\\s+", "");

					if (lineInfo != null && !lineInfo.isEmpty()
							&& (isInteger(lineInfo) | lineInfo.contains("Linea") | roveretoNBuses.contains(lineInfo))) {
						annotation = lineInfo;
						cacheTripId = cacheTripId + "$" + lineInfo;
					}
				}
				
//				if (Integer.valueOf(directionId) != Integer.valueOf(tripInfoGTFS.get(3))) {
//					System.err.println("directionId different from GTFS for: " + fileName + " tripId: " + gtfsTripId
//							+ "(gtfsDirectionId -> " + tripInfoGTFS.get(3) + ")");
//				}
				
				// prepare trips.txt
				trips.add(gtfsRouteId + "," + serviceId + "," + cacheTripId + ","
						+ (directionId.equalsIgnoreCase("0")?routeDescription + "- ANDATA":routeDescription + "- RITORNO") + "," + directionId + ",");

			int seq = 1;
			for (int i = numOfHeaders; i < matrix.length; i++) {

				// stoptimes.txt
				String time = matrix[i][j];
				String stopId = matrix[i][1];
				String stopName = matrix[i][0];

				if (stopId != null && stopName != null && !stopId.isEmpty() && !stopName.isEmpty()
						&& !stopIdNameMap.containsKey(stopId)) {
					stopIdNameMap.put(stopId, stopName);
				}

				boolean isImportant = true;

				if (stopName != null && stopName.startsWith("*")) {
					isImportant = false;
				}

				if (time != null && !time.isEmpty() && stopId != null && !stopId.isEmpty()) {
					stoptimes.add(cacheTripId + "," + time + ":00," + time + ":00," + stopId + "," + seq++); //stopId + "_" + agencyId  + ","
					//								+ (isImportant ? 1 : 0) 
				}
			}
			//			}
		}

		cacheStoptimes.addAll(stoptimes);
		cachTrips.addAll(trips);
		
		// generate stops.txt
		List<String> stops = new ArrayList<String>();
		int seq = 1;
		for (String stopId: stopIdNameMap.keySet()) {
			String stopName = stopIdNameMap.get(stopId);
			stops.add(stopId + "," + stopName + ",,46.396748,11.669557,1270");
			seq++;
		}
		
		for (String stopLine: stops) {
			if (!stopEntries.contains(stopLine)) {
				stopEntries.add(stopLine);		
			}
		}
		
		
		

	}

	public void init() throws IOException {
		String tripFile = pathToGTFS + "trips.txt";

		List<String[]> linesTrip = readFileGetLines(tripFile);

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
		
		// route model read from configuration
		this.routeModel = readRouteModel();

	}
	
	private RouteModel readRouteModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("tn-routemodel.json"),
				RouteModel.class);
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


}
