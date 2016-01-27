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
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Iterables;
import com.google.gdata.util.io.base.UnicodeReader;

public class Tester {

	private static FileRouteModel fileRouteModel;

	private static Map<String, String> keyStopNameValueStopId = new HashMap<String, String>();
	
	private static Map<String, String> keyStopNameValuePosition = new HashMap<String, String>();
	
	private static Map<String, Integer> stopOccurence = new HashMap<String, Integer>();
	
	private static final String pathToTTGTFS = "C:/tmp/otp/gen/17/";
	
	private static final String pathToCacheGTFS = "C:/tmp/annotated-cache/17/";
	
	private static final String UTF8_BOM = "\uFEFF";
	
	
	public static void main(String args[]) throws JsonParseException, JsonMappingException, IOException {
		
		String stopFile = pathToTTGTFS + "stops.txt";

		List<String[]> stops = readFileGetLines(stopFile);
		
		List<String> gtfsStopNames = new ArrayList<String>();

		for (int i = 0; i < stops.size(); i++) {
			
			String stopName = stops.get(i)[1].toLowerCase();
			
			if (!keyStopNameValuePosition.containsKey(stopName)) {
				stopOccurence.put(stopName, 1);
				if (!gtfsStopNames.contains(stopName)) {
					gtfsStopNames.add(stopName);
				}
				keyStopNameValuePosition.put(stopName, stops.get(i)[3] + "," + stops.get(i)[4]);
				keyStopNameValueStopId.put(stopName, stops.get(i)[0]);
			} else {
				int times = stopOccurence.get(stopName) + 1;
				stopOccurence.put(stopName, times);
			}

		}		
		
		
		String stopCacheGTFSFile = pathToCacheGTFS + "stops.txt";
		
		List<String[]> CStops = readFileGetLines(stopCacheGTFSFile);

		List<String> matchStops = new ArrayList<String>();
		
		int found = 0;
		int NotFound = 0;
		int totalNrOfMultipleOccurence = 0;
		for (int i = 0; i < CStops.size(); i++) {
			
			String stopName = CStops.get(i)[1].toLowerCase();
			stopName = stopName.replaceAll("\\s+", " ");
			stopName = stopName.replace("(","-");
			stopName = stopName.replace(")","");
			stopName = stopName.replace(" - part.", "");
			stopName = stopName.replace(" - arr.", "");
//			stopName = stopName.replace(".", " ");
			if (gtfsStopNames.contains(stopName)) {
				
				matchStops.add(stopName);
				found++;
				
				if (stopOccurence.get(stopName) > 1) {
					System.out.println("," + CStops.get(i)[1] + ",,,,");
					totalNrOfMultipleOccurence++;
				} else {
					System.out.println(keyStopNameValueStopId.get(stopName) + "," + CStops.get(i)[1] + ",," + keyStopNameValuePosition.get(stopName) + ",");	
				}
			} else {
				NotFound++;
				System.out.println("," + CStops.get(i)[1] + ",,,,");
			}
		}
		
		
//		int totalNrOfMultipleOccurence = 0;
//		for (String key: stopOccurence.keySet()) {
//			int occurence = stopOccurence.get(key);
//			
//			if (occurence > 1) {
//				totalNrOfMultipleOccurence++;
//				System.err.println(key + " occured " + occurence + " times");
//			}
//			
//		}
		
		System.out.println("Matched stops: " + found);
		System.err.println("Unmatched stops: " + NotFound);
		System.err.println("multiple occurence: " + totalNrOfMultipleOccurence);

//		fileRouteModel = readFileRouteConfigurationModel();
//		System.out.println(fileRouteModel.getAgencies().get(0).getFileRouteMappings().size());
//		
//		int[][] ar = new int[][]
//	            {
//	            { 0, 1, 2 },
//	            { 3, 4, 5 },
//	            { 6, 7, 8 } };
//	        ArrayList<ArrayList<Integer>> a = new ArrayList<>(ar.length);
//	        ArrayList<Integer> blankLine = new ArrayList<>(ar.length * 2 - 1);
//	        for (int i = 0; i < ar.length * 2 - 1; i++)
//	        {
//	            blankLine.add(0);
//	        }
//
//	        for (int i = 0; i < ar.length; i++)
//	        {
//	            ArrayList<Integer> line = new ArrayList<>();
//	            for (int j = 0; j < ar[i].length; j++)
//	            {
//	                line.add(ar[i][j]);
//	                if (j != ar[i].length - 1)
//	                    line.add(0);
//	            }
//	            a.add(line);
//	            if (i != ar.length - 1)
//	                a.add(blankLine);
//	        }
//
//	        for (ArrayList<Integer> b : a)
//	        {
//	            System.out.println(b);
//	        }
		
		// read stop.txt of ftp.
		
		// create map.
		
		// try to match the stop with generated stop.txt

		
	}
	
	private static FileRouteModel readFileRouteConfigurationModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("file-route.json"),
				FileRouteModel.class);
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
