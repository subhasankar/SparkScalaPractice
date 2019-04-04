package com.panda.spark.demo.entities;

import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class Test {
    public static void main(String[] args) {
        int [] a=new int[10];
        System.out.println(a[0]);
        Map<Integer,String> m=new TreeMap<>();
        //s.
        m.forEach((k,v)->{
            System.out.println(v);
        });
    }

    public static int chooseFlask(List<Integer> requirements, int m, List<List<Integer>> markings) {
        // Write your code here
        int maxM=Integer.MAX_VALUE;
        //List<Integer> l=new ArrayList<>();

        for (int i=0;i<m;i++){
            int sum=0;
            for (int r = 0; r< requirements.size(); r++) {
                int diffmax=Integer.MAX_VALUE;
                for (int j = 0; j < markings.get(i).size(); j++) {
                    if(r<markings.get(i).get(j)){
                        int diff=markings.get(i).get(j)-r;
                        if(diff<diffmax){
                            diffmax=diff;
                        }

                    }
                }
                sum=sum+diffmax;

            }
            if(sum<maxM){
                maxM=i;
            }
        }
        return maxM;
    }

    static int hourglassSum(int[][] arr) {
        int max=Integer.MIN_VALUE;
                for(int i=0;i<arr.length-2;i++){
                    for(int j=0;j<arr.length-2;j++){
                       int sum= arr[i][j]+arr[i][j+1]+arr[i][j+2]+
                                arr[i+1][j+1]+
                                arr[i+2][j]+arr[i+2][j+1]+arr[i+2][j+2];
                        if (sum>max){
                            max=sum;
                        }
                    }

                }

return max;


}
}

