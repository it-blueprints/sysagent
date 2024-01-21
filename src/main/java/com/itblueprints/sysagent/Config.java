package com.itblueprints.sysagent;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "com.itblueprints.system-agent")
@Getter @Setter
public class Config {

    private int heartBeatSecs;
    private int batchPageSize;
}       