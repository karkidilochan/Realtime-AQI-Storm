import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ZipcodeBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String zipcode = tuple.getStringByField("zipcode");
        System.out.println("Received Zipcode: " + zipcode);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // something here
    }
}
