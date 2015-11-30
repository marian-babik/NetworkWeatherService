package edu.uchicago.networkweather;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class PacketLossInterceptor implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(PacketLossInterceptor.class);

	private static SiteMapper mapper=new SiteMapper();
	
	private static JsonParser parser = new JsonParser();
	final Charset charset = Charset.forName("UTF-8");

	public PacketLossInterceptor() {
	}

	public void initialize() {
	}

	public void configure(Context context) {
	}

	public List<Event> intercepts(Event event) {

		// This is the event's body
		String body = new String(event.getBody());
//		LOG.debug(body);
		
		JsonObject jBody;
		try {
			jBody = parser.parse(body).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			LOG.error("problem in parsing msg body");
			return null;
		}

		String source, destination,ma;
		Map<String, String> newheaders = new HashMap<String, String>(8);
		
		try {
			source = jBody.get("meta").getAsJsonObject().get("source").toString().replace("\"", "");
			destination = jBody.get("meta").getAsJsonObject().get("destination").toString().replace("\"", "");
			ma = jBody.get("meta").getAsJsonObject().get("measurement_agent").toString().replace("\"", "");
			
			newheaders.put("src", source);
			newheaders.put("dest", destination);
			newheaders.put("MA", ma);
			
		} catch (Exception e) {
			LOG.warn("problem in parsing meta info." + e.toString());
			return null;
		}
		
		MappingPair<String,String> srcSite=mapper.getSite(source);
		MappingPair<String,String> destSite=mapper.getSite(destination);
		
		if (srcSite!=null){
			newheaders.put("srcSite", srcSite.getSite());
			newheaders.put("srcVO", srcSite.getVO());
		}
		if (destSite!=null){
			newheaders.put("destSite", destSite.getSite());
			newheaders.put("destVO", destSite.getVO());
		}	
		
		if (! jBody.has("summaries")){
			LOG.warn("this event has no summaries of any kind.");
			return null;
		}
		

		newheaders.put("srcProduction",mapper.getProductionThroughput(source).toString());
		newheaders.put("destProduction",mapper.getProductionThroughput(destination).toString());
		
		
		JsonArray summaries = jBody.get("summaries").getAsJsonArray();
		
		JsonArray results = null;
		for (int ind = 0; ind < summaries.size(); ind++) {
			JsonObject sum = summaries.get(ind).getAsJsonObject();
			if (sum.get("summary_window").getAsString().equalsIgnoreCase("300")) {
				results = sum.get("summary_data").getAsJsonArray();
				break;
			}
		}
		
		if (results.size()==0){
			LOG.warn("message has no summary in 5 min intervals.");
			return null;
		}else{
			LOG.debug("results:" + results.toString());
		}
		
		List<Event> measurements = new ArrayList<Event>(results.size());
		
		for (int ind = 0; ind < results.size(); ind++) {
			Long ts = results.get(ind).getAsJsonArray().get(0).getAsLong() * 1000;
			newheaders.put("timestamp", ts.toString());
			Float packetLoss = results.get(ind).getAsJsonArray().get(1).getAsFloat();
			newheaders.put("packet_loss", packetLoss.toString());
			String bod = "";
			Event evnt=EventBuilder.withBody(bod.getBytes(charset), newheaders);
//			LOG.debug(evnt.toString());
			measurements.add(evnt);
		}

		return measurements;
	}

	public Event intercept(Event event) {
		 LOG.warn("SINGLE EVENT PROCESSING");
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		LOG.debug("got " + events.size() + " events");
		List<Event> interceptedEvents = new ArrayList<Event>(events.size());
		for (Event event : events) {
			List<Event> remadeEvents=intercepts(event);
			if (remadeEvents!=null){
				interceptedEvents.addAll(remadeEvents);
			}
		}
		LOG.info("Returned " + interceptedEvents.size());
		return interceptedEvents;
	}

	public void close() {
		return;
	}

	public static class Builder implements Interceptor.Builder {

		public void configure(Context context) {
			// Retrieve property from flume conf
			// hostHeader = context.getString("hostHeader");
		}

		public Interceptor build() {
			return new PacketLossInterceptor();
		}
	}

}