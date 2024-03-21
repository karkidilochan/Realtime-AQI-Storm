package WeatherUP;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WeatherBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String zipCode = tuple.getStringByField("zipcode");
        System.out.println(zipCode);

        /*
         * // Parse weather data and extract state-wise information
         * // Assuming weather data format: "<state>:<weather>"
         * String weatherData = input.getStringByField("weatherData");
         * String[] parts = weatherData.split(":");
         * if (parts.length == 2) {
         * String state = parts[0].trim();
         * String weather = parts[1].trim();
         * 
         * // Store state-wise weather data in the map
         * stateWeatherData.put(state, weather);
         * 
         * // Log the state-wise weather data
         * System.out.println("State: " + state + ", Weather: " + weather);
         * }
         */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO: write the report
    }
}
