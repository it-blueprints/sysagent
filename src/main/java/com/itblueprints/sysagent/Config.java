package com.itblueprints.sysagent;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "com.itblueprints.sysagent")
@Getter @Setter
public class Config {

    private int heartBeatSecs = 10;
    private int workerThreads = 4;
    private int batchPageSize = 200;
}       