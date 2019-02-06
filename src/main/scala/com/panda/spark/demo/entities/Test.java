package com.panda.spark.demo.entities;

public class Test {
    public static void main(String[] args) {
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
