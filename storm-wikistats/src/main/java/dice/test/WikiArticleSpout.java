package dice.test;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import java.net.URL;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorException;


public class WikiArticleSpout extends BaseRichSpout {

  SpoutOutputCollector _collector;
  HashMap<String, Boolean> _control;
  XMLStreamReader _reader;


  private BufferedReader getSource(String address)
      throws CompressorException, IOException {
    URL url = new URL(address);
    BufferedInputStream bis = new BufferedInputStream(url.openStream());
    CompressorInputStream input = new CompressorStreamFactory()
      .createCompressorInputStream(bis);
    BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
    return br2;
  }

  private Values generateTuple() {
    /* Data storage */
    String title = null;
    String content = null;

    /* Current event name */
    String name;

    try {
      while (_reader.hasNext()) {
        switch (_reader.next()) {
          case XMLStreamReader.START_ELEMENT:
            name = _reader.getLocalName();
            if (_control.containsKey(name))
              _control.put(name, true);
            break;

          case XMLStreamReader.END_ELEMENT:
            name = _reader.getLocalName();
            if (_control.containsKey(name))
              _control.put(name, false);

            if (name == "page")
              return new Values(title, content);
            break;

          case XMLStreamReader.CHARACTERS:
            if (!_control.get("page"))
              break;

            if (_control.get("title"))
              title = _reader.getText();
            else if (_control.get("text"))
              content = _reader.getText();
            break;
        }
      }
    } catch (XMLStreamException e) {
      _collector.reportError(e);
      Utils.sleep(100);
    }
    return null;
  }

  @Override
  public void open(Map conf, TopologyContext ctx, SpoutOutputCollector coll) {
    _collector = coll;

    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_COALESCING, true);

    try {
      String address = (String)conf.get("wiki.dump.address");
      _reader = factory.createXMLStreamReader(getSource(address));
    } catch (CompressorException | IOException | XMLStreamException e) {
      coll.reportError(e);
      Utils.sleep(1000);
    }

    _control = new HashMap<String, Boolean>();
    _control.put("page", false);
    _control.put("title", false);
    _control.put("text", false);
  }

  @Override
  public void nextTuple() {
    Values tuple = generateTuple();
    if (tuple != null)
      _collector.emit(tuple);
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("title", "content"));
  }

}
