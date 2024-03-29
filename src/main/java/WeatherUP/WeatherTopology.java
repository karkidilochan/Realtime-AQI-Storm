package WeatherUP;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Path;

public class WeatherTopology {

    public static void main(String[] args) throws Exception {

        // ConfigurableTopology.start(new WeatherTopology(), args);

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
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("weather-spout", new WeatherSpout(zipCodes));
        // TODO: change the shuffle grouping for the zipcode bolt
        builder.setBolt("weather-bolt", new WeatherLossyCountBolt()).fieldsGrouping("weather-spout",
                new Fields("index"));
        builder.setBolt("report-bolt", new WeatherReportBolt()).fieldsGrouping("weather-bolt", new Fields("index"));

        Config conf = new Config();
        conf.setDebug(false);

        if (!isLocal) {
            conf.setNumWorkers(4);
            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("current-topology", conf, builder.createTopology());
            Thread.sleep(2000000); // Placehold for sleep
            cluster.shutdown();
        }
    }

}
