package dice.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class LinkCounterBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String title = tuple.getString(0);
    String body = tuple.getString(1);
    String no_int_body = body.replaceAll("\\[\\[", "");
    String no_ext_body = no_int_body.replaceAll("\\[", "");
    int int_links = (body.length() - no_int_body.length()) / 2;
    int ext_links = no_int_body.length() - no_ext_body.length();
    collector.emit(new Values(title, int_links, ext_links));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("title", "int_links", "ext_links"));
  }

}
