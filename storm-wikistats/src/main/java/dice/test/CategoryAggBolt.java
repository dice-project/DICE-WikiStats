package dice.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;


public class CategoryAggBolt extends BaseBasicBolt {
  HashMap<String, Integer> counts = new HashMap<String, Integer>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String category = tuple.getString(0);
    Integer n = counts.get(category);
    n = n == null ? 1 : n + 1;
    counts.put(category, n);
    collector.emit(new Values(category, n));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("name", "no_pages"));
  }

}
