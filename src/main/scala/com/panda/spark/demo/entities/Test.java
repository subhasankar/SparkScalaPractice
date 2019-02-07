package com.panda.spark.demo.entities;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;

public class Test {
    public static void main(String[] args) {


        new Date(2018,01,8);
        System.out.println(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toEpochSecond());
        Integer [] ints={3,2,7,46,65,76,3,1,34,1};
        int min=Integer.MAX_VALUE;
        int max=0;
        for (Integer anInt : ints) {
            if (anInt<min){
                min=anInt;
            }
            if (anInt>max){
                max=anInt;
            }
        }
        System.out.println(min+"_"+max+"with diff"+(max-min));
    }
}
