package com.etleap;

import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

/**
 * @uthor: pavanachanta
 */
public class SPaceSavingTopNCascadingTest {


    private final static String TEST_FILE = "src/test/resources/wordcount/words.txt";
    private final static String EXPECTED_OUTPUT_UNLIMITED_SPACE = "src/test/resources/wordcount/expected-output-unlimited.txt";
    private final static String EXPECTED_OUTPUT_LIMITED_SPACE = "src/test/resources/wordcount/expected-output-limited.txt";
    private final static String OUT_CASCADING = "out-cascading-wc";


    @Test
    public void testTopNWithUnlimitedSpace() throws Exception {
        SpaceSavingTopNCascading.main(new String[]{TEST_FILE, OUT_CASCADING,"100000000"});
        String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
        String expectedOutput = getOutputAsText(EXPECTED_OUTPUT_UNLIMITED_SPACE);

        //should match exact result when given unlimited space..
        assertEquals(expectedOutput, outCascading);
    }


    @Test
    public void testTopNWithLimitedSpace() throws Exception {
        SpaceSavingTopNCascading.main(new String[]{TEST_FILE, OUT_CASCADING,"10"});
        String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
        String expectedOutput = getOutputAsText(EXPECTED_OUTPUT_LIMITED_SPACE);

        //should give approximate result when given unlimited space..
        assertEquals(expectedOutput, outCascading);
    }




    public String getReducerOutputAsText(String outputDir) throws IOException {
        return getOutputAsText(outputDir + "/part-r-00000");
    }

    public String getOutputAsText(String outFile) throws IOException {
        return Files.toString(new File(outFile), Charset.forName("UTF-8"));
    }


}
