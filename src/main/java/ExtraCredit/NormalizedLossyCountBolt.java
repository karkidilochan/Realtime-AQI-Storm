package ExtraCredit;

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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class NormalizedLossyCountBolt extends BaseRichBolt {

    private final Map<String, Map<String, Item>> counts = new HashMap<>();
    private OutputCollector collector;
    private int capacity;
    private int bucket;
    private long items;

    private final double EPSILON = 0.2;
    private final double THRESHOLD = 0.2;

    private Map<String, Double> stateWeights = new HashMap<>();

    public NormalizedLossyCountBolt(Map<String, Double> stateWeights) {
        this.stateWeights = stateWeights;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.capacity = (int) (1 / EPSILON);
        this.bucket = 1;
        this.items = 0;
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("coverage", "state", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        int emitFrequency = 5;
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && input.getSourceStreamId()
                .equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            forward();
        } else {
            if (++items % capacity == 0) {
                // process entire bucket and then run delete phase.
                String coverage = input.getStringByField("coverage");
                delete(coverage);
                ++bucket;
            }
            insert(input);
        }
    }


    private void insert(Tuple input) {
        String state = input.getStringByField("state");
        String coverage = input.getStringByField("coverage");

        Map<String, Item> stateCounts = counts.computeIfAbsent(coverage, k -> new HashMap<>());

        Item item = stateCounts.get(state);
        if (item == null) {
            item = new Item(bucket - 1);
        }
        item.increment();

        stateCounts.put(state, item);
        counts.put(coverage, stateCounts);
    }



    private void delete(String coverage) {
        if (counts.containsKey(coverage)) {
            counts.get(coverage).values().removeIf(value -> value.deconstruct(bucket));
        }
    }


    private void forward() {

        for (String coverage : counts.keySet()) {
            filter(coverage);
            int size = Math.min(counts.size(), 100);

            if (size == 0) {
                return;
            }
            Map<String, Item> output = sortMapDescending(counts.get(coverage), size);

            for (Entry<String, Item> entry : output.entrySet()) {
                long normalizedCount = (long) (entry.getValue().actual() * stateWeights.get(entry.getKey()));
                collector.emit(
                        new Values(coverage, entry.getKey(), normalizedCount));
            }
        }

    }


    private void filter(String coverage) {
        int total = counts.get(coverage).size();
        counts.get(coverage).values()
                .removeIf(value -> value.frequency < (THRESHOLD
                        - EPSILON) / total);
    }

    private final static class Item implements Comparable<Item> {

        private final int delta;
        private long frequency = 0L;

        private Item(int delta) {
            this.delta = delta;
        }

        private long actual() {
            return frequency + delta;
        }

        private void increment() {
            frequency++;
        }

        private boolean deconstruct(int currentBucket) {
            return frequency + delta <= currentBucket;
        }

        @Override
        public int compareTo(Item o) {
            return Long.compare(this.actual(), o.actual());
        }

        @Override
        public String toString() {
            return "( " + frequency + ", " + delta + " )";
        }
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
