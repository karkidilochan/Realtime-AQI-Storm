package Forecast;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.tuple.Fields;

public class ForecastTopology {

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;

        String filePath = args[0];
        Path inputPath = Paths.get(filePath);
        System.out.println(inputPath.toString());

        // Read zip codes from the CSV file
        List<String> zipCodes = new ArrayList<>();
        Files.lines(inputPath).skip(1).forEach(line -> {
            String[] parts = line.split(",");
            zipCodes.add(parts[0]);
        });

        // Setup topology
        int executorTasks = 4;
        int threads = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("forecast-spout", new ForecastSpout(zipCodes), threads);
        builder.setBolt("count-bolt", new ForecastLossyCountBolt(), threads)
                .setNumTasks(executorTasks)
                .fieldsGrouping("forecast-spout", new Fields("coverage"));
        builder.setBolt("report-bolt", new ForecastReportBolt(), threads)
                        .setNumTasks(executorTasks);

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_DEBUG, false);

        if (!isLocal) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("forecast-topology", conf, builder.createTopology());
            Thread.sleep(2000000); // Placehold for sleep
            cluster.shutdown();
        }
    }

}

