package dice.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;


public class CategoriesExtractBolt extends BaseBasicBolt {

  private static HashSet<String> findCategories(String text) {
    HashSet<String> matches = new HashSet<String>();
    Pattern p = Pattern.compile("\\[Category:([^\\|\\]]+)\\]");
    Matcher m = p.matcher(text);
    while (m.find()) {
      matches.add(m.group(1));
    }
    return matches;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String title = tuple.getString(0);
    String content = tuple.getString(1);
    HashSet<String> categories = findCategories(content);
    if (! categories.isEmpty())
      collector.emit(new Values(title, categories));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("title", "categories"));
  }

}
