package org.pd.streaming.connector.es;

import java.time.LocalDateTime;

import lombok.Data;

@Data
public class SampleMessage
{
    String metric_name;
    Long metric_value;
    String timestamp;
    
    public SampleMessage(String name, Long value)
    {
        this.metric_name = name;
        this.metric_value = value;
        this.timestamp = LocalDateTime.now().toString();
    }
}
