package com.itblueprints.sysagent;

public class SystemAgentException extends RuntimeException{

    public SystemAgentException(String message){
        super(message);
    }

    public SystemAgentException(String message, Throwable cause){
        super(message, cause);
    }
}
