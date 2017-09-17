package dice.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;


public class CategoriesSplitBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String title = tuple.getString(0);
    HashSet<String> categories = (HashSet<String>)tuple.getValue(1);
    for (String cat : categories)
      collector.emit(new Values(cat, title));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("category", "title"));
  }

}
