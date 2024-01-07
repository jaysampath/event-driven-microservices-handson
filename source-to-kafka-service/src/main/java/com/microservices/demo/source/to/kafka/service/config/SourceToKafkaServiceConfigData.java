package com.microservices.demo.source.to.kafka.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "source-to-kafka-service")
public class SourceToKafkaServiceConfigData {
    private List<String> tweetKeywords;
    private String welcomeMessage;
    private Long mockSleepMs;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
}
