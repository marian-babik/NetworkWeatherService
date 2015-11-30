package edu.uchicago.networkweather;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class ThroughputInterceptor implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(ThroughputInterceptor.class);

	private static JsonParser parser = new JsonParser();
	final Charset charset = Charset.forName("UTF-8");

	private static SiteMapper mapper=new SiteMapper();
	
	public ThroughputInterceptor() {
	}

	public void initialize() {
	}

	public void configure(Context context) {
	}

	public List<Event> intercepts(Event event) {

		// This is the event's body
		String body = new String(event.getBody());
		LOG.debug(body);
		
		JsonObject jBody;
		try {
			jBody = parser.parse(body).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			LOG.error("problem in parsing msg body");
			return null;
		}


		Map<String, String> newheaders = new HashMap<String, String>(8);
		 
		String source = jBody.get("meta").getAsJsonObject().get("source").toString().replace("\"", "");
		String destination = jBody.get("meta").getAsJsonObject().get("destination").toString().replace("\"", "");
		String ma = jBody.get("meta").getAsJsonObject().get("measurement_agent").toString().replace("\"", "");
		
		newheaders.put("src", source);
		newheaders.put("dest", destination);
		newheaders.put("MA", ma);
		
		
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

		newheaders.put("srcProduction",mapper.getProductionThroughput(source).toString());
		newheaders.put("destProduction",mapper.getProductionThroughput(destination).toString());
		
		String body1 = "{";

		
		Set<Entry<String, JsonElement>> datapoints = jBody.get("datapoints").getAsJsonObject().entrySet() ;
		
		List<Event> measurements = new ArrayList<Event>(datapoints.size());
		
		for (Map.Entry<String, JsonElement> entry : datapoints)
		{
		    Long ts=Long.parseLong(entry.getKey())*1000;
		    Float thr = entry.getValue().getAsFloat();
			LOG.debug("throughput: " + ts + "/" + thr);

			newheaders.put("timestamp", ts.toString());
			
			String bod = body1 +"\"throughput\":"+ thr.toString() + "}";
			LOG.debug(bod);

			Event evnt=EventBuilder.withBody(bod.getBytes(charset), newheaders);
			
			LOG.debug(newheaders.toString());
			
			measurements.add(evnt);
		}
		
		return measurements;
	}

	public Event intercept(Event event) {
		 LOG.warn("SINGLE EVENT PROCESSING");
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		LOG.debug("got " + events.size() + " events.");
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
			return new ThroughputInterceptor();
		}
	}

}