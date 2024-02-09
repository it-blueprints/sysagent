package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.MapData;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.Map;

@Getter
@Setter
public class Partition extends MapData {

    private Integer partitionNum;

    private Integer totalPartitions;

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static Partition from(Map<String, Object> map){
        val args = new Partition();
        args.loadFrom(map);
        return args;
    }
}
