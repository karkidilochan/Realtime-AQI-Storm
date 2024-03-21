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

import zipcodes.ZipcodeBolt;
import zipcodes.ZipcodeSpout;

public class ForecastTopology {

    public static void main(String[] args) throws Exception {

        boolean isLocal = true;

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
        builder.setSpout("forecast-spout", new ForecastSpout(zipCodes));
        builder.setBolt("count-bolt", new LossyCountBolt(), threads)
                .setNumTasks(executorTasks)
                .fieldsGrouping("forecast-spout", new Fields("state"));
        builder.setBolt("report-bolt", new ReportBolt())
                .globalGrouping("count-bolt");

        Config conf = new Config();
        conf.setDebug(true);

        if (!isLocal) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("zipcode-topology", conf, builder.createTopology());
            Thread.sleep(2000000); // Placehold for sleep
            cluster.shutdown();
        }
    }

}
