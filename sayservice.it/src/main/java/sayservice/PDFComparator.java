package sayservice;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class PDFComparator {

	private static final String pathToFolder1 = "src/test/resources/inputtimetable/12";
	private static final String pathToFolder2 = "C:/deleted/daily-work/08.10.15/csv-Trento/csv-Trento";

	private static boolean verbose = true;

	public static void main(String[] args) {

		File folder1 = new File(pathToFolder1);
		File folder2 = new File(pathToFolder2);

		for (final File fileEntry1 : folder1.listFiles()) {
			if (fileEntry1.isDirectory() | fileEntry1.getName().contains(".json")
					| fileEntry1.getName().contains(".zip")) {
				continue;
			} else {
				boolean exist = false;
				for (File fileEntry2 : folder2.listFiles()) {
					if (fileEntry2.getName().equalsIgnoreCase(fileEntry1.getName())) {
						if (verbose)
							System.out.println("Comparison in process for ->  " + fileEntry1.getName());
						exist = true;
						PDFComparator.compareFile(fileEntry1, fileEntry2);
					}
				}
				if (!exist) {
					System.err.println(fileEntry1.getName());
				}
			}
		}
	}

	private static void compareFile(File fileEntry1, File fileEntry2) {

		try {
			List<String> lines1 = Files.asCharSource(fileEntry1, Charsets.UTF_8).readLines();
			List<String> lines2 = Files.asCharSource(fileEntry2, Charsets.UTF_8).readLines();
			
			if (!lines1.containsAll(lines2) | !lines2.containsAll(lines1)) {
				
				List<String> different = new ArrayList<String>();
				
				if (!lines1.containsAll(lines2)) {
					different.addAll(lines2);
					different.removeAll(lines1);
				} else if (!lines2.containsAll(lines1)) {
					different.addAll(lines1);
					different.removeAll(lines2);
				}
				
				for (String diff: different) {
					System.err.println(diff);
				}
			}
		        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
