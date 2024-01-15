package com.microservices.demo.source.to.kafka.service.runner.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.microservices.demo.config.SourceToKafkaServiceConfigData;
import com.microservices.demo.source.to.kafka.service.listener.TweetKafkaStatusListener;
import com.microservices.demo.source.to.kafka.service.model.Status;
import com.microservices.demo.source.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final SourceToKafkaServiceConfigData sourceToKafkaServiceConfigData;

    private final TweetKafkaStatusListener tweetKafkaStatusListener;

    private final ObjectMapper objectMapper;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };

    private static final String tweetAsRawJson = "{" +
            "\"createdAt\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWEET_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(SourceToKafkaServiceConfigData configData,
                                 TweetKafkaStatusListener tweetKafkaStatusListener,
                                 ObjectMapper objectMapper) {
        this.sourceToKafkaServiceConfigData = configData;
        this.tweetKafkaStatusListener = tweetKafkaStatusListener;
        this.objectMapper = objectMapper;
    }

    @Override
    public void start() {
        final String[] keywords = sourceToKafkaServiceConfigData.getTweetKeywords().toArray(new String[0]);
        final int minTweetLength = sourceToKafkaServiceConfigData.getMockMinTweetLength();
        final int maxTweetLength = sourceToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = sourceToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock filtering tweet streams for keywords {}", Arrays.toString(keywords));
        simulateTweetStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTweetStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    LOG.info("Generated Tweet- {}", formattedTweetAsRawJson);
                    Status status = objectMapper.readValue(formattedTweetAsRawJson, Status.class);
                    tweetKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (Exception e) {
                LOG.error("Error generating tweet!", e);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while sleeping for waiting new status to create!!");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWEET_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;

        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

}
