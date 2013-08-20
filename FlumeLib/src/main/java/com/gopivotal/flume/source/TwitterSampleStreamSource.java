/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.gopivotal.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSampleStreamSource extends AbstractSource implements
		EventDrivenSource, Configurable {

	private static final Logger LOG = Logger
			.getLogger(TwitterSampleStreamSource.class);

	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	private String authUrl;
	private String accessTokenUrl;
	private String requestTokenUrl;

	private TwitterStream stream = null;;

	@Override
	public void configure(Context context) {

		// Get all necessary parameters from the Flume configuration
		consumerKey = context.getString("consumer.key");
		consumerSecret = context.getString("consumer.secret");
		accessToken = context.getString("access.token");
		accessTokenSecret = context.getString("access.token.secret");
		authUrl = context.getString("access.token.url");
		accessTokenUrl = context.getString("auth.url");
		requestTokenUrl = context.getString("request.token.url");
	}

	@Override
	public void start() {

		ChannelProcessor channel = getChannelProcessor();

		stream = getTwitterStream();

		// Add a new listener to the stream
		stream.addListener(new TwitterEventChannelListener(channel));

		LOG.info("Starting Twitter sample stream...");
		stream.sample();

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
