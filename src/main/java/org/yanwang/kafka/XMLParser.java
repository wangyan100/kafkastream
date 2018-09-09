/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.yanwang.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * @author yanwang this xmlparser will get text content of the tag
 */
public class XMLParser {

    private static final Logger logger = LoggerFactory.getLogger(XMLParser.class);

    static private DocumentBuilder documentBuilder;

    public static boolean checkMatchs(String textContent, String matchePattern) {
        boolean matched = false;
        try {
            String[] tokens = matchePattern.split(",");
            for (String token : tokens) {
                matched = (textContent.trim().equalsIgnoreCase(token.trim()));
                if (matched) {
                    break;
                }
            }
            return matched;
        } catch (Exception e) {
            logger.error("Exception occured ", e);
            return matched;
        }
    }


    public static String getTextContentFromTagLocalName(String xmlString, String nameSpace, String localName) {
        String textContent = null;
        try {

            if (documentBuilder == null) {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                documentBuilder = documentBuilderFactory.newDocumentBuilder();
            }

            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(xmlString));

            Document doc = documentBuilder.parse(is);
            NodeList nodeList = doc.getElementsByTagNameNS(nameSpace, localName);
            if (nodeList.getLength() > 0) {
                textContent = nodeList.item(0).getTextContent();
            }
            return textContent;
        } catch (IOException | ParserConfigurationException | DOMException | SAXException e) {
            logger.error("Error happened at XMLParser ", e);
            return textContent;
        }
    }
}
