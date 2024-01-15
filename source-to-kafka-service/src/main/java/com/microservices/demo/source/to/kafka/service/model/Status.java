package com.microservices.demo.source.to.kafka.service.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.Date;

@Data
public class Status {
    private long id;
    private String text;
    private TweetUser user;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "EEE MMM dd HH:mm:ss zzz yyyy")
    private Date createdAt;
}
