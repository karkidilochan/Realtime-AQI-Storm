package Forecast;

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

public class LossyCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = -7619769952927707443L;

    private final Map<String, Item> counts = new HashMap<>();
    private OutputCollector collector;
    private int capacity;
    private int bucket;
    private long items;

    private final double EPSILON = 0.2;
    private final double THRESHOLD = 0.2;

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
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        int emitFrequency = 10;
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
                delete();
                ++bucket;
            }
            insert(input);
        }
    }

    /**
     * insert the items to D updating the ones that exist or creating a
     * new entry (e, 1, b - 1) <br>
     * <br>
     * e : current word <br>
     * b : current bucket
     * 
     * @param input
     */
    private void insert(Tuple input) {
        String s = input.getStringByField("hash");
        Item item = counts.get(s);
        if (item == null) {
            item = new Item(bucket - 1);
        }
        item.increment();
        counts.put(s, item);
    }

    /**
     * items from the current D where f + d <= b <br>
     * <br>
     * f : frequency of word <br>
     * d : delta value of b - 1 when seen <br>
     * b : current bucket
     */
    private void delete() {
        counts.values().removeIf(value -> value.deconstruct(bucket));
    }

    /**
     * The top 100 true frequencies of e : f + delta <br>
     * <br>
     * f : frequency of word <br>
     * d : delta value of b - 1 when seen <br>
     * 
     */
    private void forward() {

        filter();

        int size = counts.size() > 100 ? 100 : counts.size();
        if (size == 0) {
            return;
        }
        Map<String, Item> output = sortMapDescending(counts, size);
        for (Entry<String, Item> entry : output.entrySet()) {
            collector.emit(
                    new Values(entry.getKey(), entry.getValue().actual()));
        }
    }

    /**
     * Only emit those entries in data structure D, where f >= (s - e) / N
     * <br>
     * <br>
     * f : frequency of word <br>
     * s : threshold (between 0 - 1) <br>
     * e : epsilon <br>
     * N : total number of items in D data structure D <br>
     * 
     */
    private void filter() {
        int total = counts.size();
        counts.values()
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
