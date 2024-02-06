package com.itblueprints.sysagent;

public class SysAgentException extends RuntimeException{

    public SysAgentException(String message){
        super(message);
    }

    public SysAgentException(String message, Throwable cause){
        super(message, cause);
    }
}
