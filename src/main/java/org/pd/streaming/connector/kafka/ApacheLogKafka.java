package org.pd.streaming.connector.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApacheLogKafka
{
    String ident;
    Geoip geoip;
    String request;
    String response;
    String clientip;
    String version;
    String host;
    String timestamp;
    String agent;
    String message;
    String bytes;
    String path;
    String referrer;
    String auth;
    String verb;
    String httpversion;
}