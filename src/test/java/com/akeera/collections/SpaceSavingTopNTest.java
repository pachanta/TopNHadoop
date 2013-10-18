package com.akeera.collections;



import com.akeera.collections.Bucket;
import com.akeera.collections.SpaceSavingTopN;
import org.junit.Test;

import java.util.Hashtable;

import static org.junit.Assert.assertEquals;

/**
 * @author: pavanachanta
 */

public class SpaceSavingTopNTest {


    @Test
    public void testAddElement(){

        SpaceSavingTopN<String> topN = new SpaceSavingTopN<String>(3);

        String[] testSeq = new String[]{"X","Y","Y","Z","X"};

        topN = addSeq(topN,testSeq);

        System.out.println(topN);
    }


    private SpaceSavingTopN addSeq(SpaceSavingTopN<String> TopNImpl, String[] seq){

        for(String s : seq){
            TopNImpl.add(s);
        }
        return TopNImpl;
    }


    @Test
    public void testSmallerInputWithLimit(){

        final int MAX_COUNTERS = 7;
        String input = "AWBCHZMSLSURTSJVBNAHBSLJVSDPQABAS" ;

        SpaceSavingTopN<String> topN = new SpaceSavingTopN<String>(MAX_COUNTERS);

        for(char c : input.toCharArray()){
            topN.add(String.valueOf(c));
            System.out.println(topN);
        }

        Hashtable<String,Integer> expected = new Hashtable<String,Integer>();
        expected.put("S",6);
        expected.put("B",3);
        expected.put("A",2);
        expected.put("Q",1);
        expected.put("P",1);
        expected.put("D",1);
        expected.put("V",1);

        assertTopNEquals(topN,expected);


    }




    private void  assertTopNEquals(SpaceSavingTopN<String> topN,Hashtable<String,Integer> expected){

        for(String s : topN.getElementIndex().keySet()){
            Bucket b = (Bucket)topN.getElementIndex().get(s).parent.item;
            assertEquals(expected.get(s).intValue(),b.count);
        }

    }


    @Test
    public void testBiggerInputWithoutLimit(){

        final int MAX_COUNTERS = 26;

        String input = "AWBCHZMSLSURTSJVBNAHBSLJVSDPQABAS" +
                       "UYGFAJLNDSVHAUYQGRFQYTCVZHWRUJFNV" +
                       "APOUWEYRACBLKNVIUAGWRFGABBVAUGAQB" +
                       "CNZMSGDHJKLZAERWTSGMBKJBVZKBAOFUB" +
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZNOWIKNO"  ;

        SpaceSavingTopN<String> topN = new SpaceSavingTopN<String>(MAX_COUNTERS);

        int[] counts = new int[26];

        for(char c : input.toCharArray()){
            counts[c - 65]++;
            topN.add(String.valueOf(c));
        }

        assertEquals(26,topN.getElementIndex().keySet().size());
        //Note: The count results should be accurate when we set the limit higher than number of counters we are expecting
        for(String s : topN.getElementIndex().keySet()){
            Bucket b = (Bucket)topN.getElementIndex().get(s).parent.item;
            assertEquals(counts[s.charAt(0) -65],b.count);
        }
    }

}
