package chimpstorm.examples.github;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.*;
import storm.trident.operation.builtin.*;
import storm.trident.testing.VisibleMemoryMapState;
import storm.trident.testing.Tap;

import chimpstorm.storm.trident.operations.JsonParse;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.infochimps.storm.trident.spout.FileBlobStore;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.StartPolicy;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import java.util.*;
import java.io.IOException;

import com.infochimps.wukong.state.WuEsState;

public class GithubTopology {
  public static class ExtractLanguageCommits extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractLanguageCommits.class);

    public void execute(TridentTuple tuple, TridentCollector collector){
      JsonNode node = (JsonNode) tuple.getValue(0);
      if(!node.get("type").toString().equals("\"PushEvent\"")) return;
      if (node.get("repository") != null &&
          node.get("repository").get("language") != null &&
          node.get("payload") != null &&
          node.get("payload").get("size") != null) {
        List values = new ArrayList(2);
        //grab the language and the action
        values.add(node.get("repository").get("language").asText());
        values.add(node.get("payload").get("size").asLong());
        collector.emit(values);
      }
      return;
    }
  }

  /*
   * Create and run the Github topology
   * The topology:
   * 1) reads the event stream from the github spout
   * 2) parses the JSON
   * 3) Extracts the langauge and commit count from the JSON
   * 4) Groups by language
   * 5) ... TBD ...
   */
  public static void main(String[] args) throws Exception, InvalidTopologyException {
    IBlobStore bs = new FileBlobStore("/Users/eric/data/github/gz");
    OpaqueTransactionalBlobSpout spout = new OpaqueTransactionalBlobSpout(bs, StartPolicy.EARLIEST, null);

    TridentTopology topology = new TridentTopology();
    topology.newStream("github-activities", spout)
        .each(new Fields("line"), new JsonParse(), new Fields("parsed-json"))
            //.each(new Fields("parsed-json"), new Tap());
        .each(new Fields("parsed-json"), new ExtractLanguageCommits(), new Fields("language", "commits"))
            //.each(new Fields("language","commits"), new Tap());
        .groupBy(new Fields("language"))
        .persistentAggregate(new VisibleMemoryMapState.Factory(), new Fields("commits"), new Sum(), new Fields("commit-sum"))
        .newValuesStream()
        .each(new Fields("language","commit-sum"), new Tap());

    Config conf = new Config();
    // Process one batch at a time, waiting 2 seconds between, and a 5 second batch timeout
    // conf.setMaxSpoutPending(4);
    conf.setMessageTimeoutSecs(3);
    // conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
    // conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS,  2000);
    System.out.println("Topology created");
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("lang-counter", conf, topology.build());
  }
}
