package storm.petrel.dummy;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ActivityStreamsSampleSpout extends BaseRichSpout {

    private final Random random = new Random();
    private SpoutOutputCollector collector;
    private List<String> sampleData;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            sampleData = loadSampleData("dummy/as.sample.json");
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        collector.emit(randomSample(sampleData, 1));
    }

    private List<String> loadSampleData(String path) throws IOException{
        BufferedReader reader = readerForClasspathResource(path);
        List<String> lines = Lists.newArrayList();

        String line;

        while((line = reader.readLine()) != null) {
            lines.add(line);
        }

        return lines;
    }

    private BufferedReader readerForClasspathResource(String path) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        return new BufferedReader(new InputStreamReader(is));
    }

    private List<Object> randomSample(List<String> elements, int count) {
        List<Object> randomSample = Lists.newArrayList();
        for(int i = 0; i < count; i++) {
            randomSample.add(randomElement(elements));
        }
        return randomSample;
    }

    private String randomElement(List<String> elements) {
        return elements.get(random.nextInt(elements.size()));
    }
}
