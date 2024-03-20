import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.List;
import java.util.Map;

public class ZipcodeSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> zipCodes;
    private int index = 0;

    public ZipcodeSpout(List<String> zipCodes) {
        this.zipCodes = zipCodes;
        System.out.println("Length of Zipcode: " + zipCodes.size());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (index >= zipCodes.size()) {
            // Stop emitting if all zip codes have been sent
            return;
        }

        // Emit each zip code
        this.collector.emit(new Values(zipCodes.get(index)));
        index++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zipcode"));
    }
}
