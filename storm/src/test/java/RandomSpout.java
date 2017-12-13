import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("randomstring"));
    }
}
