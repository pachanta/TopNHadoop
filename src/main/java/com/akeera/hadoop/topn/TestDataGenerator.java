package com.akeera.hadoop.topn;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: pavanachanta
 * Date: 10/5/13
 * Time: 7:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestDataGenerator {

    public static String testDataFile = "/Users/pavanachanta/git/etleap/TopNHadoop/data/test.data";

    public static int DATA_SIZE    = 1000000;
    public static int DATA_RANGE   = 2000;

    public static void main(String[] args){


        Random rn = new Random();
        try{
            FileWriter writer = new FileWriter(testDataFile);
            for(int i=0;i < DATA_SIZE; i++){
                writer.write(rn.nextInt(DATA_RANGE) + "\n");
            }
            }catch (Exception e){
                e.printStackTrace();
            }


    }
}
