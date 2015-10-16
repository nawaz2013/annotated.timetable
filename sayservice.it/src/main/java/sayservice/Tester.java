package sayservice;

import java.io.IOException;
import java.util.ArrayList;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Tester {

	private static FileRouteModel fileRouteModel;
	
	public static void main(String args[]) throws JsonParseException, JsonMappingException, IOException {
		fileRouteModel = readFileRouteConfigurationModel();
		System.out.println(fileRouteModel.getAgencies().get(0).getFileRouteMappings().size());
		
		int[][] ar = new int[][]
	            {
	            { 0, 1, 2 },
	            { 3, 4, 5 },
	            { 6, 7, 8 } };
	        ArrayList<ArrayList<Integer>> a = new ArrayList<>(ar.length);
	        ArrayList<Integer> blankLine = new ArrayList<>(ar.length * 2 - 1);
	        for (int i = 0; i < ar.length * 2 - 1; i++)
	        {
	            blankLine.add(0);
	        }

	        for (int i = 0; i < ar.length; i++)
	        {
	            ArrayList<Integer> line = new ArrayList<>();
	            for (int j = 0; j < ar[i].length; j++)
	            {
	                line.add(ar[i][j]);
	                if (j != ar[i].length - 1)
	                    line.add(0);
	            }
	            a.add(line);
	            if (i != ar.length - 1)
	                a.add(blankLine);
	        }

	        for (ArrayList<Integer> b : a)
	        {
	            System.out.println(b);
	        }

		
	}
	
	private static FileRouteModel readFileRouteConfigurationModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("file-route.json"),
				FileRouteModel.class);
	}
	
  }
