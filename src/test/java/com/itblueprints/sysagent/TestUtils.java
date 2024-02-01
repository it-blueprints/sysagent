package com.itblueprints.sysagent;

import lombok.val;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {
    //------------------------------------------------------------
    public static <T> void assertTrueForAll(List<T> items, Predicate<T> pred){
        val count = items.stream().filter(pred).count();
        assertEquals(items.size(), count);
    }
}
