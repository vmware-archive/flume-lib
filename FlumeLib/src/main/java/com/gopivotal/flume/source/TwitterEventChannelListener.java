package com.gopivotal.flume.source;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import twitter4j.RawStreamListener;

public class TwitterEventChannelListener implements RawStreamListener {

	private ChannelProcessor channel = null;

	private Map<String, String> headers = new HashMap<String, String>();
	private JsonFactory factory = new JsonFactory();
	private ObjectMapper mapper = new ObjectMapper(factory);
	private SimpleDateFormat sf = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss ZZZZZ yyyy");

	/**
	 * Initializes a new event listener that pushes events to the given Flume
	 * ChannelProcessor
	 * 
	 * @param channel
	 *            The processor for events
	 */
	public TwitterEventChannelListener(ChannelProcessor channel) {
		this.channel = channel;
		sf.setLenient(true);
	}

	@Override
	public void onMessage(String rawString) {

		try {
			// Only push new status events
			if (rawString.contains("created_at")) {

				// Decode the line to JSON to get the timestamp
				JsonNode node = mapper.readTree(rawString);

				// Get the timestamp and add the header
				headers.put("timestamp", String.valueOf(sf.parse(
						node.get("created_at").getTextValue()).getTime()));

				// Push the event through the channel
				channel.processEvent(EventBuilder.withBody(
						rawString.getBytes(), headers));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onException(Exception ex) {
		ex.printStackTrace();
	}
}
