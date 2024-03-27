package WeatherUP;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WeatherReportBolt extends BaseRichBolt {
    private final Map<String, Map<String, Long>> counts = new HashMap<>();

    private BufferedWriter buffer;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
        try {
            buffer = new BufferedWriter(
                    new FileWriter("current_aqi_log.txt", true));
        } catch (IOException e) {
            System.out.println("Error while writing log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        /* emit tick tuples every 10 seconds */
        // TODO: frequency should be 60 seconds
        int emitFrequency = 15;
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && input.getSourceStreamId()
                        .equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            /* only write if its a tick tuple */
            write();
        } else {
            /* TODO: fix the fields */
            String index = input.getStringByField("index");
            Map<String, Long> stateCounts = counts.computeIfAbsent(index, k -> new HashMap<>());

            stateCounts.put(input.getStringByField("state"),
                    input.getLongByField("count"));
            counts.put(index, stateCounts);
        }

    }

    private void write() {
        System.out.println("Write called");
        int size = Math.min(counts.size(), 6);
        if (size == 0) {
            return;
        }

        for (String index : counts.keySet()) {
            StringBuilder sb = new StringBuilder(Instant.now().toString());
            sb.append(" ");

            Map<String, Long> output = sortMapDescending(counts.get(index), 5);

            counts.get(index).clear();

            sb.append(index + " ");

            for (Entry<String, Long> entry : output.entrySet()) {
                sb.append(entry.getKey()).append(" ").append(entry.getValue())
                        .append(" ");
            }
            System.out.println(sb.toString() + "\n");
            try {
                buffer.write(sb.toString() + "\n");
                buffer.flush();
            } catch (IOException e) {
                System.out.println("Error while writing log: " + e.getMessage());
                e.printStackTrace();
            }
        }
        ;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public <K, V extends Comparable<V>> Map<K, V> sortMapDescending(
            Map<K, V> map, int size) {
        return map.entrySet().stream()
                .sorted(Map.Entry
                        .comparingByValue(Comparator.reverseOrder()))
                .limit(size)
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue, (e1, e2) -> e1,
                        LinkedHashMap::new));
    }

}
