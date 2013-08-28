package com.gopivotal.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSampleStreamSource extends AbstractSource implements
		EventDrivenSource, Configurable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TwitterSampleStreamSource.class);

	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	private String authUrl;
	private String accessTokenUrl;
	private String requestTokenUrl;
	private String[] filter = null;

	private TwitterStream stream = null;;

	@Override
	public void configure(Context context) {

		// Get all necessary parameters from the Flume configuration
		consumerKey = context.getString("consumer.key");
		consumerSecret = context.getString("consumer.secret");
		accessToken = context.getString("access.token");
		accessTokenSecret = context.getString("access.token.secret");
		authUrl = context.getString("access.token.url",
				"https://api.twitter.com/oauth/request_token");
		accessTokenUrl = context.getString("auth.url",
				"https://api.twitter.com/oauth/authorize");
		requestTokenUrl = context.getString("request.token.url",
				"https://api.twitter.com/oauth/access_token");
		String strFilter = context.getString("filter.by", null);

		filter = (strFilter != null) ? strFilter.split(",") : null;

		// Log the properties
		LOG.info("Properties: {}", context.getParameters());
	}

	@Override
	public void start() {

		ChannelProcessor channel = getChannelProcessor();

		stream = getTwitterStream();

		// Add a new listener to the stream
		stream.addListener(new TwitterEventChannelListener(channel));

		if (filter == null) {
			LOG.info("Starting Twitter sample stream...");
			stream.sample();
		} else {
			LOG.info("Starting Twitter filtering on {}", (Object[]) filter);
			FilterQuery query = new FilterQuery();
			query.track(filter);
			stream.filter(query);
		}

		super.start();
	}

	@Override
	public void stop() {

		if (stream != null) {
			LOG.info("Shutting down Twitter stream");
			stream.shutdown();
		} else {
			LOG.info("Twitter stream is null");
		}

		super.stop();
	}

	/**
	 * Gets the configured Twitter Stream
	 * 
	 * @return the Twitter Stream
	 */
	private TwitterStream getTwitterStream() {

		ConfigurationBuilder twitterConf = new ConfigurationBuilder();

		twitterConf.setOAuthConsumerKey(consumerKey);
		twitterConf.setOAuthConsumerSecret(consumerSecret);

		twitterConf.setOAuthAccessToken(accessToken);
		twitterConf.setOAuthAccessTokenSecret(accessTokenSecret);

		twitterConf.setDebugEnabled(true);
		twitterConf.setJSONStoreEnabled(true);

		twitterConf.setOAuthAccessTokenURL(accessTokenUrl);
		twitterConf.setOAuthAuthorizationURL(authUrl);
		twitterConf.setOAuthRequestTokenURL(requestTokenUrl);

		OAuthAuthorization oauth = new OAuthAuthorization(twitterConf.build());

		return new TwitterStreamFactory().getInstance(oauth);
	}
}
