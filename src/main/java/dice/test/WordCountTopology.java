package dice.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import dice.test.SentenceSpout;
import dice.test.SplitSentenceBolt;
//import dice.test.WordCountBolt;

import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.fields;
import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;


public class WordCountTopology {

  static final String KEYSPACE = "bla";
  static final String TABLE = "heh";

  private static void createSchema() {
    // Ugly as hell, but should do for now.
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    Session session = cluster.connect();
    session.execute(  "create keyspace " + KEYSPACE + " with replication = {"
                    + "'class': 'SimpleStrategy', "
                    + "'replication_factor': 3"
                    + "};");
    session.execute(  "create table " + KEYSPACE + "." + TABLE + " ("
                    + "  id text primary key,"
                    + "  word text"
                    + ");");
  }

  private static StormTopology buildTopology() {
    String query = "insert into " + TABLE + " (id, word) values (?, ?)";

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new SentenceSpout(), 5);
    builder.setBolt("split", new SplitSentenceBolt(), 8)
           .shuffleGrouping("spout");
    builder.setBolt("marker", new MarkerBolt(), 12)
           .fieldsGrouping("split", new Fields("word"));
    builder.setBolt("store", new CassandraWriterBolt(
                      async(simpleQuery(query).with(fields("id", "word")))))
           .shuffleGrouping("marker");

    return builder.createTopology();
  }


  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Provide topology name");
      System.exit(1);
    }

    createSchema();

    Config conf = new Config();
    conf.setNumWorkers(3);
    conf.put("cassandra.keyspace", KEYSPACE);
    StormSubmitter.submitTopology(args[0], conf, buildTopology());

    // TODO: Not sure why application hangs at the end if this is not present.
    System.exit(1);
  }
}

