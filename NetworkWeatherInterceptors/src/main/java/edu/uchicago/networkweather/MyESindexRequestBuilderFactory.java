package edu.uchicago.networkweather;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.AbstractElasticSearchIndexRequestBuilderFactory;
//import org.apache.flume.sink.elasticsearch.DocumentIdBuilder;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;

/**
 * An extended serializer for flume events into the same format LogStash uses
 * This adds some more features on top of the default ES serializer that is part of 
 * the Flume distribution.
 * 
 * @note This builder will not work when using the REST client of the ES sink 
 * of Flume 1.5.X. The REST client does not use a builder.
 *
 */
public class MyESindexRequestBuilderFactory extends AbstractElasticSearchIndexRequestBuilderFactory {

	private ElasticSearchEventSerializer serializer = new MyESserializer();
//	private DocumentIdBuilder docIdBuilder = (DocumentIdBuilder)serializer;

	public MyESindexRequestBuilderFactory() {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));
	}

	public MyESindexRequestBuilderFactory(FastDateFormat fd) {
		super(fd);
	}

	public MyESindexRequestBuilderFactory(ElasticSearchEventSerializer serializer) {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));

		this.serializer = serializer;
	}

	public MyESindexRequestBuilderFactory(ElasticSearchEventSerializer serializer, FastDateFormat fd) {
		super(fd);
		this.serializer = serializer;
	}

	@Override
	public void configure(Context context) {
		serializer.configure(context);
	}

	@Override
	public void configure(ComponentConfiguration config) {
		serializer.configure(config);
	}

	@Override
	protected void prepareIndexRequest(IndexRequestBuilder indexRequest, String indexName, String indexType, Event event) throws IOException {
		BytesStream contentBuilder = serializer.getContentBuilder(event);
		BytesReference contentBytes = contentBuilder.bytes();
		indexRequest.setIndex(indexName).setType(indexType).setSource(contentBytes);
//		String hashId = docIdBuilder.getDocumentId(contentBytes);
//		if (null != hashId && !hashId.isEmpty())
//			indexRequest.setId(hashId.toString());
	}

}