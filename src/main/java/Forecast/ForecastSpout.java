package Forecast;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class ForecastSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private List<String> zipCodes;

    private String apiUrl = "http://api.weatherapi.com/v1/current.json";

    private String apiKey = "";

    private int index = 0;

    public ForecastSpout(List<String> zipcodes) {
        this.zipCodes = zipcodes;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (this.zipCodes != null) {
            // make an api call and emit the output of weather api
            try {
                URL url = new URL(this.apiUrl + "?key=" + this.apiKey + "&q=" + zipCodes.get(index));
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");

                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                connection.disconnect();

                /*
                 * TODO: uncomment this to send api response to the bolts
                 * also parse the received weather data before sending it
                 */
                // this.collector.emit(new Values(response.toString()));
            } catch (Exception e) {
                System.out.println("Error while sending get request to weather api: " + e.getMessage());
                e.printStackTrace();
            }

            // TODO: change this to the weather api response and send to the bolt
            // currently sending zipcode to make the work flow, while printing api outputs
            this.collector.emit(new Values(zipCodes.get(index)));
            index++;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zipcode"));

        // TODO: uncomment this
        // declarer.declare(new Fields("weatherData"));
    }
}
