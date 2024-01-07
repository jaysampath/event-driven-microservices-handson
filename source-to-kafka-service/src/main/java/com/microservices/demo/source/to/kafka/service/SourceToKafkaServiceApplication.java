package com.microservices.demo.source.to.kafka.service;

import com.microservices.demo.source.to.kafka.service.config.SourceToKafkaServiceConfigData;
import com.microservices.demo.source.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class SourceToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SourceToKafkaServiceApplication.class);

    private final SourceToKafkaServiceConfigData sourceToKafkaServiceConfigData;

    private final StreamRunner streamRunner;

    public SourceToKafkaServiceApplication(SourceToKafkaServiceConfigData configData,
                                            StreamRunner runner) {
        this.sourceToKafkaServiceConfigData = configData;
        this.streamRunner = runner;
    }

    public static void main(String[] args) {
        SpringApplication.run(SourceToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts...");
        LOG.info(Arrays.toString(sourceToKafkaServiceConfigData.getTweetKeywords().toArray(new String[] {})));
        LOG.info(sourceToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
