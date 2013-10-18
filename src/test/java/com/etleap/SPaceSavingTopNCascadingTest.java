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
    private final static String EXPECTED_OUTPUT = "src/test/resources/wordcount/expected-output.txt";
    private final static String OUT_CASCADING = "out-cascading-wc";


    @Test
    public void testCascading() throws Exception {
        SpaceSavingTopNCascading.main(new String[]{TEST_FILE, OUT_CASCADING,"24"});
        String outCascading = getOutputAsText(OUT_CASCADING + "/part-00000");
        String expectedOutput = getOutputAsText(EXPECTED_OUTPUT);
        assertEquals(expectedOutput, outCascading);
    }




    public String getReducerOutputAsText(String outputDir) throws IOException {
        return getOutputAsText(outputDir + "/part-r-00000");
    }

    public String getOutputAsText(String outFile) throws IOException {
        return Files.toString(new File(outFile), Charset.forName("UTF-8"));
    }


}
