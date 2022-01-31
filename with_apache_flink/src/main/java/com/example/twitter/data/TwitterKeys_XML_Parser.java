package com.example.twitter.data;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class TwitterKeys_XML_Parser {
    static String value_="";
    static String key_="";
    public static String get_data(String token){
		try {
			File file = new File("/home/rita/Desktop/twitter_keys/twitter_keys.xml");

			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(file);
			doc.getDocumentElement().normalize();

            Element root = doc.getDocumentElement();
            NodeList nList = doc.getElementsByTagName("add");

            for (int temp = 0; temp < nList.getLength(); temp++)
            {
                Node node = nList.item(temp);
                if (node.getNodeType() == Node.ELEMENT_NODE)
                {
                    Element eElement = (Element) node;
                    key_=eElement.getAttribute("key");
                    value_=eElement.getAttribute("value");

                    if(key_.equals(token)){
                        return value_;
                    }
                } 
            }
		}
		catch (Exception e) {
			System.out.println(e);
		}
        finally{
        }
        return value_;
	}

}



