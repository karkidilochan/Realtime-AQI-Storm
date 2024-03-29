package Forecast;

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

public class ForecastSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private List<String> zipCodes;

    private String apiUrl = "http://api.weatherapi.com/v1/forecast.json";

    private String apiKey = "92997c8a8c38446bac804408241503";

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
            if (index >= zipCodes.size()) {
                return;
            }
            // make an api call and emit the output of weather api
            try {


                JsonNode forecastResponse = getAPIResponse();

                // Extract state name
                String state = forecastResponse.path("location").path("region").asText();

                // Extract us-epa-index
                JsonNode forecastData = forecastResponse.get("forecast").get("forecastday");


                for (int i = 0; i < 3; i++) {
                    String coverage = aggregateCoverage(forecastData.get(i).get("hour"));
                    this.collector.emit(new Values(state, coverage));
                }

                /*
                 * TODO: uncomment this to send api response to the bolts
                 * also parse the received weather data before sending it
                 */

                index++;
            } catch (Exception e) {
                System.out.println("Error while sending get request to weather api: " + e.getMessage());
                e.printStackTrace();
            }

            // TODO: change this to the weather api response and send to the bolt
            // currently sending zipcode to make the work flow, while printing api outputs
            // this.collector.emit(new Values(zipCodes.get(index)));
            // index++;
        }
    }

    private JsonNode getAPIResponse() throws Exception {
        URL urlString = new URL(this.apiUrl + "?key=" + this.apiKey + "&q=" + zipCodes.get(index)
                + "&days=3&aqi=no&alerts=no");

        HttpURLConnection connection = (HttpURLConnection) urlString.openConnection();
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

        return rootNode;
    }

    private String aggregateCoverage(JsonNode hourData) {
        int cloudCoverage = 0;
        for (int i = 0; i < 24; i++) {
            cloudCoverage += hourData.get(i).get("cloud").asInt();
        }
        int cloudAverage = convertToLevel(cloudCoverage / 24);
        return String.valueOf(cloudAverage);
    }

    private int convertToLevel(int cloudCoverage) {
        if (cloudCoverage >= 0 && cloudCoverage <= 19) {
            return 1;
        } else if (cloudCoverage >= 20 && cloudCoverage <= 39) {
            return 2;
        } else if (cloudCoverage >= 40 && cloudCoverage <= 59) {
            return 3;
        } else if (cloudCoverage >= 60 && cloudCoverage <= 79) {
            return 4;
        } else if (cloudCoverage >= 80 && cloudCoverage <= 100) {
            return 5;
        } else {
            throw new IllegalArgumentException("Cloud coverage value must be between 0 and 100");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("state", "coverage"));

        // TODO: uncomment this
        // declarer.declare(new Fields("weatherData"));
    }
}
