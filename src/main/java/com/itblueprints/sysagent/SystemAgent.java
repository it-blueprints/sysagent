package com.itblueprints.sysagent;

import org.springframework.context.annotation.ComponentScan;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
@ComponentScan("com.itblueprints.sysagent")
public @interface SystemAgent {
}
