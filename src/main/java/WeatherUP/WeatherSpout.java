package WeatherUP;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class WeatherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private List<String> zipCodes;

    private String apiUrl = "http://api.weatherapi.com/v1/current.json";

    private String apiKey = "92997c8a8c38446bac804408241503";

    private int index = 0;

    public WeatherSpout(List<String> zipcodes) {
        this.zipCodes = zipcodes;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (this.zipCodes != null) {
            if(index >= zipCodes.size()) {
                return;
            }
            // make an api call and emit the output of weather api
            try {
                URL url = new URL(this.apiUrl + "?key=" + this.apiKey + "&q=" + zipCodes.get(index) + "&aqi=yes");
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

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode rootNode = objectMapper.readTree(response.toString());

                // Extract state name
                String state = rootNode.path("location").path("region").asText();

                // Extract us-epa-index
                String usEpaIndex = rootNode.path("current").path("air_quality").path("us-epa-index").toString();

                /*
                 * TODO: uncomment this to send api response to the bolts
                 * also parse the received weather data before sending it
                 */
                 this.collector.emit(new Values(state, usEpaIndex));
                 index++;
            } catch (Exception e) {
                System.out.println("Error while sending get request to weather api: " + e.getMessage());
                e.printStackTrace();
            }

            // TODO: change this to the weather api response and send to the bolt
            // currently sending zipcode to make the work flow, while printing api outputs
//            this.collector.emit(new Values(zipCodes.get(index)));
//            index++;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("state", "index"));

        // TODO: uncomment this
        // declarer.declare(new Fields("weatherData"));
    }
}
