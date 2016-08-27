package dice.test;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.HashMap;

import java.net.URL;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorException;


public class Reader {
//public class WikiArticleSpout {

  private static BufferedReader getSource(String address)
      throws CompressorException, IOException {
    URL url = new URL(address);
    BufferedInputStream bis = new BufferedInputStream(url.openStream());
    CompressorInputStream input = new CompressorStreamFactory()
      .createCompressorInputStream(bis);
    BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
    return br2;
  }

  private static void emit(String title, String text) {
    System.out.printf("EMIT: (%s, %s...)\n",
                      title, text.substring(0, Math.min(50, text.length())));
  }

  public static void main(String[] args) throws XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_COALESCING, true);

    String address = "http://172.16.93.126:17890/enwiki-20160801-pages-articles2.xml-p000030304p000088444.bz2";
    BufferedReader source = null;
    try {
      source = getSource(address);
    } catch (CompressorException e) {
      e.printStackTrace();
      System.exit(1);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    XMLStreamReader reader = null;
    try {
      reader = factory.createXMLStreamReader(source);
    } catch (XMLStreamException e) {
      e.printStackTrace();
      System.exit(1);
    }

    HashMap<String, Boolean> control = new HashMap<>();
    control.put("page", false);
    control.put("title", false);
    control.put("text", false);

    /* Data storage */
    String title = null;
    String text = null;

    /* Current event name */
    String name;

    while (reader.hasNext()) {
      switch (reader.next()) {
        case XMLStreamReader.START_ELEMENT:
          name = reader.getLocalName();
          if (control.containsKey(name))
            control.put(name, true);
          break;

        case XMLStreamReader.END_ELEMENT:
          name = reader.getLocalName();
          if (control.containsKey(name))
            control.put(name, false);

          if (name == "page")
            emit(title, text);
          break;

        case XMLStreamReader.CHARACTERS:
          if (!control.get("page"))
            break;

          if (control.get("title"))
            title = reader.getText();
          else if (control.get("text"))
            text = reader.getText();
          break;
      }
    }
  }

}
