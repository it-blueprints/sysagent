package com.itblueprints.sysagent;

import lombok.val;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Dbg {

    private static boolean on = true;
    public static void on(){ on = true; }
    public static void off(){ on = false; }

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void p(String label, Object o) {
        if (on) {
            try {
                System.out.println("* " + label + ": " + o);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void p(Object o) {
        if (on) System.out.println("* " + o);
    }

    public static void p(String label, LocalDateTime t) {
        if (on) {
            try {
                System.out.println("* " + label + ": " + formatter.format(t));
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void pTime(String label, long millis) {
        if (on) {
            try {
                val t = Utils.millisToDateTime(millis);
                System.out.println("* " + label + ": " + formatter.format(t));
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
