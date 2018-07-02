package io.mewbase.eventsource.impl.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseEventSink {

    final Connection connection = ConnectionFactory.createConnection();

    public HbaseEventSink() throws IOException {
    }

}
