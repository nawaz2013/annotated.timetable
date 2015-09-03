package sayservice;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Tester {

	private static FileRouteModel fileRouteModel;
	
	public static void main(String args[]) throws JsonParseException, JsonMappingException, IOException {
		fileRouteModel = readFileRouteConfigurationModel();
		System.out.println(fileRouteModel.getAgencies().get(0).getFileRouteMappings().size());
	}
	
	private static FileRouteModel readFileRouteConfigurationModel() throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper().readValue(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("file-route.json"),
				FileRouteModel.class);
	}
}
