package org.pd.streaming.connector.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data 
@JsonIgnoreProperties(ignoreUnknown = true)
public class Geoip
{
    @JsonProperty("country_code2")
    String countryCode2;
    @JsonProperty("continent_code")
    String continentCode;
    @JsonProperty("ip")
    String ip;
    @JsonProperty("latitude")
    Double latitude;
    @JsonProperty("country_code3")
    String countryCode3;
    @JsonProperty("location")
    Location location;
    @JsonProperty("longitude")
    Double longitude;
    @JsonProperty("country_name")
    String countryName;
    @JsonProperty("timezone")
    String timezone;
}