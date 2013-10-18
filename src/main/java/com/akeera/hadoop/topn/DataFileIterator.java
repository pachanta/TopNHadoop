package com.akeera.hadoop.topn;

import java.io.BufferedReader;
import java.util.Iterator;

/**
 * @author: pavanachanta
 */

public class DataFileIterator implements Iterator<String>
{
    BufferedReader reader;

    DataFileIterator(BufferedReader myReader) {
        reader = myReader;
    }

    @Override
    public boolean hasNext() {
        try{
            return reader.ready();
        }catch(Exception e){
            return false;
        }
    }


    @Override
    public String next() {
        try{
            return reader.readLine();
        }catch(Exception e){
            return null;
        }

    }

    @Override
    public void remove() {
    }
}
