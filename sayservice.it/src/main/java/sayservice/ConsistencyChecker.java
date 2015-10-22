package sayservice;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.util.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class ConsistencyChecker {

//	private static final String pathToAnnotatedCSVs = "C:/deleted/daily-work/07.05.15/otp-0.15/cache/annotated/16/";
	private static final String pathToAnnotatedCSVs = "src/test/resources/annotatedtimetable/12/";

	private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm");
	
	private static final String ignorePattern = "2015062620150909-annotated.csv";

	public static void main(String[] args) {
		ConsistencyChecker checker = new ConsistencyChecker();
		File folder = new File(pathToAnnotatedCSVs);

		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory() | fileEntry.getName().contains(".json") | fileEntry.getName().contains(".zip")
					| fileEntry.getName().endsWith(ignorePattern)) {
				continue;
			} else {
//				System.out.println("Checking in process for ->  " + fileEntry.getName());
				checker.check(pathToAnnotatedCSVs + fileEntry.getName());
			}
		}
	}

	private void check(String... files) {

		try {
			for (String fileName : files) {
				File file = new File(fileName);
				List<String> lines = Files.asCharSource(file, Charsets.UTF_8).readLines();
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

				consistencyCheck(matrix, maxNumberOfCols, fileName);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void consistencyCheck(String[][] output, int noOfOutputCols, String fileName) {
		boolean inconsistent = false;
		try {
			for (int j = 2; j < noOfOutputCols - 1; j++) {
				for (int i = 11; i < (output.length - 1); i++) {
					String currTime = output[i][j];
					if (currTime != null && !currTime.isEmpty()) {
						Date curr = TIME_FORMAT.parse(currTime);
						for (int iNext = i + 1; iNext < output.length; iNext++) {
							String nextTime = output[iNext][j];
							if (nextTime != null && !nextTime.isEmpty() && !nextTime.startsWith("-")) {
								Date next = TIME_FORMAT.parse(nextTime);
								if (curr.after(next)) {
									System.out.println(output[i][0]);
									inconsistent = true;
									System.err.println(fileName + " time: " + currTime + " is before: " + nextTime
											+ " (" + i + "," + j + ")");
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

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}

}
