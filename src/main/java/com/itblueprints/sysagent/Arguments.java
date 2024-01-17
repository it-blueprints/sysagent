package com.itblueprints.sysagent;

public class Arguments extends MapData {

    public void add(Arguments other){
        this.data.putAll(other.data);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
