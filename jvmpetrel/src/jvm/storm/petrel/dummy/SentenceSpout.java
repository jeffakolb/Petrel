package storm.petrel.dummy;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SentenceSpout extends BaseRichSpout {

    private static final List<String> lines = Arrays.asList(
        "Pug drinking vinegar authentic asymmetrical tofu, banh mi chillwave American Apparel biodiesel gastropub",
        "Semiotics master cleanse jean shorts Vice, salvia small batch 3 wolf moon sartorial pop-up",
        "Wayfarers Blue Bottle actually beard Banksy, Portland cardigan roof party crucifix Pitchfork",
        "Retro Cosby sweater letterpress gastropub, fanny pack ugh cardigan sustainable Godard beard seitan",
        "Fashion axe 3 wolf moon Banksy chia stumptown cardigan",
        "Lomo squid cornhole retro pickled Wes Anderson",
        "Neutra beard locavore, Portland mixtape Shoreditch lo-fi photo booth kale chips"
    );

    private Random random = new Random();

    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String line = lines.get(random.nextInt(lines.size()));
        collector.emit(Arrays.<Object>asList(line));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }
}
