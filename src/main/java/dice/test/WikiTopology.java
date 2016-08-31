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

import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.fields;
import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;


public class WikiTopology {

  static final String KEYSPACE = "wiki";
  static final String TBL_PAGES = "pages";
  static final String TBL_CATEGORIES = "categories";

  static final String KS_DROP = String.format(
      "drop keyspace if exists %s;",
      KEYSPACE);
  static final String KS_CREATE = String.format(
      "create keyspace %s with replication = {" +
      "  'class': 'SimpleStrategy', 'replication_factor': 1" +
      "};",
      KEYSPACE);
  static final String TBL_PAGES_CREATE = String.format(
      "create table %s.%s (" +
      "  title text primary key," +
      "  int_links int," +
      "  ext_links int," +
      "  categories set<text>" +
      ");",
      KEYSPACE, TBL_PAGES);
  static final String TBL_CATEGORIES_CREATE = String.format(
      "create table %s.%s (" +
      "  name text primary key," +
      "  no_pages int" +
      ");",
      KEYSPACE, TBL_CATEGORIES);
  static final String TBL_PAGES_STORE_LINKS = String.format(
      "insert into %s.%s (title, int_links, ext_links) values (?, ?, ?);",
      KEYSPACE, TBL_PAGES);
  static final String TBL_PAGES_STORE_CATEGORIES = String.format(
      "insert into %s.%s (title, categories) values (?, ?);",
      KEYSPACE, TBL_PAGES);
  static final String TBL_CATEGORIES_STORE_CATEGORY = String.format(
      "insert into %s.%s (name, no_pages) values (?, ?);",
      KEYSPACE, TBL_CATEGORIES);

  private static void createSchema(String endpoint) {
    Cluster cluster = Cluster.builder().addContactPoint(endpoint).build();
    Session session = cluster.connect();
    session.execute(KS_DROP);
    session.execute(KS_CREATE);
    session.execute(TBL_PAGES_CREATE);
    session.execute(TBL_CATEGORIES_CREATE);
  }

  private static StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    // Spout
    builder.setSpout("wiki", new WikiArticleSpout(), 1);

    // Page link counter branch
    builder.setBolt("count_links", new LinkCounterBolt(), 2)
      .shuffleGrouping("wiki");
    builder.setBolt("store_links", new CassandraWriterBolt(
          async(simpleQuery(TBL_PAGES_STORE_LINKS)
            .with(fields("title", "int_links", "ext_links")))), 1)
      .shuffleGrouping("count_links");

    // Page categories assignment branch
    builder.setBolt("extract_categories", new CategoriesExtractBolt(), 2)
      .shuffleGrouping("wiki");
    builder.setBolt("store_page_categories", new CassandraWriterBolt(
          async(simpleQuery(TBL_PAGES_STORE_CATEGORIES)
            .with(fields("title", "categories")))), 1)
      .shuffleGrouping("extract_categories");

    // Categories aggregation branch
    builder.setBolt("split_categories", new CategoriesSplitBolt(), 1)
      .shuffleGrouping("extract_categories");
    builder.setBolt("aggregate_category", new CategoryAggBolt(), 5)
      .fieldsGrouping("split_categories", new Fields("category"));
    builder.setBolt("store_categories", new CassandraWriterBolt(
          async(simpleQuery(TBL_CATEGORIES_STORE_CATEGORY)
            .with(fields("name", "no_pages")))), 1)
      .shuffleGrouping("aggregate_category");

    return builder.createTopology();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println(
          "Provide topology name, cassandra enpoint and wikipedia dump link.");
      System.exit(1);
    }

    createSchema(args[1]);

    Config conf = new Config();
    conf.setNumWorkers(5);
    conf.put("cassandra.keyspace", KEYSPACE);
    conf.put("cassandra.nodes", args[1]);
    conf.put("wiki.dump.address", args[2]);

    /* If running on the cluster, use next line */
    StormSubmitter.submitTopology(args[0], conf, buildTopology());

    /* If running locally, comment cluster line and uncomment lines below */
    //LocalCluster cluster = new LocalCluster();
    //cluster.submitTopology(args[0], conf, buildTopology());
    //Thread.sleep(10000);
    //cluster.killTopology(args[0]);
    //cluster.shutdown();

    // TODO: Not sure why application hangs at the end if this is not present.
    System.exit(0);
  }
}
