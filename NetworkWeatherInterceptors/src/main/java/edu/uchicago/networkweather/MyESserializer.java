package edu.uchicago.networkweather;


import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

//import org.apache.flume.sink.elasticsearch.DocumentIdBuilder;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extended serializer for flume events into the same format LogStash uses</p>
 * This adds some more features on top of the default ES serializer that is part of 
 * the Flume distribution.</p>
 * For more details see: https://github.com/gigya/flume-ng-elasticsearch-ser-ex 
 * </p>
 * Logstash format:
 * 
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@source_host": ""
 *    "@source_path": ""
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 * 
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 * 
 * <pre>
 *  message : String -> @message : String 
 *     or body : String -> @message : String     
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 * 
 */
public class MyESserializer implements ElasticSearchEventSerializer {

	private static final Logger LOG = LoggerFactory.getLogger(MyESserializer.class);
	
	public XContentBuilder getXContentBuilder(Event event) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		appendHeaders(builder, event);
		return builder;
	}

	//@Override
	public BytesStream getContentBuilder(Event event) throws IOException {
		LOG.warn("getContentBuilder called");
		return getXContentBuilder(event);
	}

	private void appendBody(XContentBuilder builder, Event event) throws IOException, UnsupportedEncodingException {
		LOG.warn("Appending Body");
		byte[] body = event.getBody();
		ContentBuilderUtilEx.appendField(builder, "@message", body, false);
	}

	private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
		Map<String, String> headers = Maps.newHashMap(event.getHeaders());
		LOG.warn("Appending Headers");
		// look for a "message" header and append as body if exists
		String message = headers.get("message");
		if (!StringUtils.isBlank(message) && StringUtils.isBlank(headers.get("@message"))) {
			ContentBuilderUtilEx.appendField(builder, "@message", message.getBytes(charset), false);
			headers.remove("message");
		} else {
			// if not, append the body as the message
			appendBody(builder, event);
		}

		String timestamp = headers.get("timestamp");
		if (!StringUtils.isBlank(timestamp) && StringUtils.isBlank(headers.get("@timestamp"))) {
			long timestampMs = Long.parseLong(timestamp);
			builder.field("@timestamp", new Date(timestampMs));
			headers.remove("timestamp");
		}

		String source = headers.get("source");
		if (!StringUtils.isBlank(source) && StringUtils.isBlank(headers.get("@source"))) {
			ContentBuilderUtilEx.appendField(builder, "@source", source.getBytes(charset));
			headers.remove("source");
		}

		String type = headers.get("type");
		if (!StringUtils.isBlank(type) && StringUtils.isBlank(headers.get("@type"))) {
			ContentBuilderUtilEx.appendField(builder, "@type", type.getBytes(charset));
			headers.remove("type");
		}

		String host = headers.get("host");
		if (!StringUtils.isBlank(host) && StringUtils.isBlank(headers.get("@source_host"))) {
			ContentBuilderUtilEx.appendField(builder, "@source_host", host.getBytes(charset));
			headers.remove("host");
		}

		String srcPath = headers.get("src_path");
		if (!StringUtils.isBlank(srcPath) && StringUtils.isBlank(headers.get("@source_path"))) {
			ContentBuilderUtilEx.appendField(builder, "@source_path", srcPath.getBytes(charset));
			headers.remove("src_path");
		}

		for (String key : headers.keySet()) {
				byte[] val = headers.get(key).getBytes(charset);
				ContentBuilderUtilEx.appendField(builder, key, val, false);
		}

	}



	public void configure(Context context) {
		LOG.warn("configure from context");		
	}

	public void configure(ComponentConfiguration conf) {
		LOG.warn("configure from ComponentConfiguration");
	}

}