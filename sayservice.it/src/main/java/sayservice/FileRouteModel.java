package sayservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileRouteModel {

	private Map<String, FileRouteAgencyModel> agencies;

	public List<FileRouteAgencyModel> getAgencies() {
		return new ArrayList<FileRouteModel.FileRouteAgencyModel>(agencies.values());
	}

	public void setAgencies(List<FileRouteAgencyModel> agencies) {
		this.agencies = new HashMap<String, FileRouteModel.FileRouteAgencyModel>();
		for (FileRouteAgencyModel a : agencies) {
			this.agencies.put(a.getAgencyId(), a);
		}
	}

	public static class FileRouteAgencyModel {
		private String agencyId;
		private Map<String, String> fileRouteMappings;

		public Map<String, String> getFileRouteMappings() {
			return fileRouteMappings;
		}

		public void setFileRouteMappings(Map<String, String> fileRouteMappings) {
			this.fileRouteMappings = fileRouteMappings;
		}

		public String getAgencyId() {
			return agencyId;
		}

		public void setAgencyId(String agencyId) {
			this.agencyId = agencyId;
		}

	}

}