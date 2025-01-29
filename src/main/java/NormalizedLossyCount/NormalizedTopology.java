package ExtraCredit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.tuple.Fields;

public class NormalizedTopology {

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;

        String filePath = args[0];
        Path inputPath = Paths.get(filePath);
        System.out.println(inputPath.toString());

        // Read zip codes from the CSV file
        List<String> zipCodes = new ArrayList<>();

        Map<String, Integer> stateCounts = new HashMap<>();

        Files.lines(inputPath).skip(1).forEach(line -> {
            String[] parts = line.split(",");
            zipCodes.add(parts[0]);
            String stateName = parts[3];
            stateCounts.put(stateName, stateCounts.getOrDefault(stateName, 0) + 1);
        });

        Map<String, Double> stateWeights = calculateStateWeights(stateCounts);

        // Setup topology
        int executorTasks = 4;
        int threads = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("forecast-spout", new NormalizedSpout(zipCodes), threads);
        builder.setBolt("count-bolt", new NormalizedLossyCountBolt(stateWeights), threads)
                .setNumTasks(executorTasks)
                .fieldsGrouping("forecast-spout", new Fields("coverage"));
        builder.setBolt("report-bolt", new NormalizedReportBolt(), threads)
                .fieldsGrouping("count-bolt", new Fields("coverage")).setNumTasks(executorTasks);

        Config conf = new Config();
        conf.setDebug(false);

        if (!isLocal) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("forecast-topology", conf, builder.createTopology());
            Thread.sleep(2000000); // Placehold for sleep
            cluster.shutdown();
        }
    }

    private static Map<String, Double> calculateStateWeights(Map<String,Integer> stateCounts) {
        int totalCount = stateCounts.values().stream().mapToInt(Integer::intValue).sum();
        Map<String, Double> stateWeights = new HashMap<>();

        for (Map.Entry<String, Integer> entry : stateCounts.entrySet()) {
            String stateName = entry.getKey();
            int count = entry.getValue();
            double weight = (double) count / totalCount;
            stateWeights.put(stateName, weight);
        }

        return stateWeights;
    }

}
