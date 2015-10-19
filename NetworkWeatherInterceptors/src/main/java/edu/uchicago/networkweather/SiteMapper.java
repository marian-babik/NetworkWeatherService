package edu.uchicago.networkweather;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SiteMapper {
	private static final Logger log = LoggerFactory.getLogger(ThroughputInterceptor.class);

	Set<String> sites = new HashSet<String>();

	// maps ips to sites
	private Map<String, MappingPair<String,String>> m = new HashMap<String, MappingPair<String,String>>();

	SiteMapper() {
		reload();
	}

	private void reload() {

		String theURL = "http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE";
		JsonArray AGISsites = getJsonFromUrl(theURL).getAsJsonArray();

		if (AGISsites == null) {
			log.error("did not load sites. Aborting.");
			System.exit(1);
		}

		for (int i = 0; i < AGISsites.size(); ++i) {
			JsonObject s = AGISsites.get(i).getAsJsonObject();
			sites.add(s.get("rc_site").getAsString());
		}

		theURL = "http://atlas-agis-api.cern.ch/request/service/query/list/?json&state=ACTIVE&type=PerfSonar";
		JsonArray ps = getJsonFromUrl(theURL).getAsJsonArray();

		if (ps == null) {
			log.error("did not load PerfSONAR endpoints. Aborting.");
			System.exit(1);
		}

		for (int i = 0; i < ps.size(); ++i) {
			JsonObject s = ps.get(i).getAsJsonObject();
			String hostname=s.get("endpoint").getAsString();
			String sitename=s.get("rc_site").getAsString();
			String ip=GetIP(hostname);
			String VO="";
			if (sites.contains(sitename)) 
				VO="ATLAS";
			MappingPair<String, String> p=new MappingPair<String,String>(ip,VO);
			m.put(ip,p);
		}

	}

	private JsonElement getJsonFromUrl(String theURL) {
		log.info("getting data from:" + theURL);
		URL url;
		HttpURLConnection request;
		JsonParser jp = new JsonParser();
		JsonElement root = null;
		try {
			url = new URL(theURL);
			request = (HttpURLConnection) url.openConnection();
			request.setConnectTimeout(60000);
			request.setReadTimeout(60000);
			request.connect();
			root = jp.parse(new InputStreamReader((InputStream) request.getContent()));
			request.disconnect();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return root;
	}

	private String GetIP(String hostname) {
		try {
			return InetAddress.getByName(hostname).getHostAddress();
		} catch (UnknownHostException e) {
			log.warn("hostname "+hostname+" unknown.");
		}
		return null;
	}
	
	public MappingPair<String,String> getSite(String ip){
		if (m.containsKey(ip)) return m.get(ip);
		return null;
	}
}
