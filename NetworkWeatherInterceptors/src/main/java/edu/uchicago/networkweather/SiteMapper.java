package edu.uchicago.networkweather;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
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

	private static Date lastReload = new Date();
	
	Set<String> sites = new HashSet<String>();
	Set<String> latencyHosts = new HashSet<String>();
	Set<String> throughputHosts = new HashSet<String>();
	
	// maps ips to sites
	private Map<String, MappingPair<String,String>> m = new HashMap<String, MappingPair<String,String>>();

	SiteMapper() {
		System.setProperty("jsse.enableSNIExtension", "false");
		reload();
	}

	private void reload() {
		
		lastReload = new Date();
		
		String theURL = "http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE";
		JsonArray AGISsites = getJsonFromUrl(theURL).getAsJsonArray();

		if (AGISsites == null) {
			log.error("did not load sites. Aborting.");
			System.exit(1);
		}
		
		sites.clear();
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
		
		m.clear();
		for (int i = 0; i < ps.size(); ++i) {
			JsonObject s = ps.get(i).getAsJsonObject();
			String hostname=s.get("endpoint").getAsString();
			String sitename=s.get("rc_site").getAsString();
			String ip=GetIP(hostname);
			String VO="";
			if (sites.contains(sitename)) 
				VO="ATLAS";
			MappingPair<String, String> p=new MappingPair<String,String>(sitename,VO);
			log.info("mapping host: "+hostname+"\tip: "+ip+"\tSite: "+sitename+"\tVO: "+VO);
			m.put(ip,p);
		}

		// loading production throughput hosts
		theURL = "https://myosg.grid.iu.edu/psmesh/json/name/wlcg-all";
		JsonElement t=getJsonFromUrl(theURL);
		if (t == null){
			log.error("did not load perfosonar throughput host names. Skipping this time.");
		}
		JsonArray lH = t.getAsJsonObject().get("organizations").getAsJsonArray();
		throughputHosts.clear();
		for (int i = 0; i < lH.size(); ++i) {
			JsonArray sites = lH.get(i).getAsJsonObject().get("sites").getAsJsonArray();
			for (int s = 0; s < sites.size(); ++s){
				JsonArray hosts = sites.get(s).getAsJsonObject().get("hosts").getAsJsonArray();
				for (int h = 0; h < hosts.size(); ++h){
					JsonArray addresses = hosts.get(h).getAsJsonObject().get("addresses").getAsJsonArray();
					for (int a = 0; a < addresses.size(); ++a){
						String address = addresses.get(a).getAsString();
						throughputHosts.add(address);
						log.info("throughput production host:", address);
					}
				}
			}
		}
		
		// loading production latency hosts
		theURL = "https://myosg.grid.iu.edu/psmesh/json/name/wlcg-latency-all";
		t=getJsonFromUrl(theURL);
		if (t == null){
			log.error("did not load perfosonar latency host names. Skipping this time.");
		}
		JsonArray tH = t.getAsJsonObject().get("organizations").getAsJsonArray();
		latencyHosts.clear();
		for (int i = 0; i < tH.size(); ++i) {
			JsonArray sites = tH.get(i).getAsJsonObject().get("sites").getAsJsonArray();
			for (int s = 0; s < sites.size(); ++s){
				JsonArray hosts = sites.get(s).getAsJsonObject().get("hosts").getAsJsonArray();
				for (int h = 0; h < hosts.size(); ++h){
					JsonArray addresses = hosts.get(h).getAsJsonObject().get("addresses").getAsJsonArray();
					for (int a = 0; a < addresses.size(); ++a){
						String address = addresses.get(a).getAsString();
						latencyHosts.add(address);
						log.info("latency production host:", address);
					}
				}
			}
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
		if (new Date().getTime() - lastReload.getTime() > 12 * 3600 * 1000)
			reload();
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

	public Boolean getProductionLatency(String hostname){
		return latencyHosts.contains(hostname);
	}
	
	public Boolean getProductionThroughput(String hostname){
		return throughputHosts.contains(hostname);
		
	}
	
}
