package com.yw.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yw.kafka.XMLParser;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import scala.App;

/**
 * Unit test for simple App.
 */
public class AppTest {
      Logger logger = LoggerFactory.getLogger(AppTest.class);

    @Test
    public void checkMatches() throws FileNotFoundException, IOException {
        logger.info("checkMatches is invoked ...");
        InputStream in = new FileInputStream(App.class.getClassLoader().getResource("message1.xml").getFile());
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder result = new StringBuilder();
        String line;
        boolean flag = false;
        String newLine = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            result.append(flag ? newLine : "").append(line);
            flag = true;
        }
        String content = XMLParser.getTextContentFromTagLocalName(result.toString(), "*", "localtagname");
        boolean matched = XMLParser.checkMatchs(content, "matched_content");
        assertTrue(matched == true);
        matched = XMLParser.checkMatchs(content, "matched_content_wrong");
        assertTrue(matched == false);
    }

    @Test
    public void getTextContent() throws FileNotFoundException, IOException {

        InputStream in = new FileInputStream(App.class.getClassLoader().getResource("message1.xml").getFile());
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder result = new StringBuilder();
        String line;
        boolean flag = false;
        String newLine = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            result.append(flag ? newLine : "").append(line);
            flag = true;
        }
        // "*" will accept all namespace 
        String content = XMLParser.getTextContentFromTagLocalName(result.toString(), "*", "localtagname");

        assertTrue(content != null);
    }

}
