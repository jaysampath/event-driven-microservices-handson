package com.microservices.demo.source.to.kafka.service.model;

import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class Status {
    private long id;
    private String text;
    private TweetUser user;
    private Long createdAt;
}
