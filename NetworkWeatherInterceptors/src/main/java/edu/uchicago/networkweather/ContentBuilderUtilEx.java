package edu.uchicago.networkweather;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.elasticsearch.common.jackson.core.JsonParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Utility methods for using ElasticSearch {@link XContentBuilder}
 */
public class ContentBuilderUtilEx {

	private static final Charset charset = Charset.defaultCharset();

	private ContentBuilderUtilEx() {
	}

	public static void appendField(XContentBuilder builder, String field, Object data) throws IOException {
		builder.field(field, data);
	}

	public static void appendField(XContentBuilder builder, String field, byte[] data) throws IOException {
		appendField(builder, field, data, false);
	}

	public static void appendField(XContentBuilder builder, String field, byte[] data, boolean allowObject)
			throws IOException {
		XContentType contentType = XContentFactory.xContentType(data);
		if (contentType == null || !allowObject) {
			addSimpleField(builder, field, data);
		} else {
			addComplexField(builder, field, contentType, data);
		}
	}

	public static void addSimpleField(XContentBuilder builder, String fieldName, byte[] data) throws IOException {
		builder.field(fieldName, new String(data, charset));
	}

	public static void addComplexField(XContentBuilder builder, String fieldName, XContentType contentType, byte[] data)
			throws IOException {
		XContentParser parser = null;
		try {
			parser = XContentFactory.xContent(contentType).createParser(data);
			Map<String, Object> map = parser.map();
			builder.field(fieldName, map);
		} catch (JsonParseException ex) {
			// If we get an exception here the most likely cause is nested JSON
			// that can't be figured out in the body. At this point just push it
			// through as is, we have already added the field so don't do it again
			addSimpleField(builder, fieldName, data);
		} finally {
			if (parser != null) {
				parser.close();
			}
		}
	}

	public static Map<String, Object> tryParsingToMap(String data) {
		XContentType contentType = XContentFactory.xContentType(data);
		if (null != contentType) {
			XContentParser parser = null;
			try {
				parser = XContentFactory.xContent(contentType).createParser(data);
				Map<String, Object> map = parser.map();
				return map;
			} catch (IOException ex) {
			} finally {
				if (parser != null) {
					parser.close();
				}
			}
		}
		return null;
	}
	
}