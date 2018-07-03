package io.mewbase.eventsource.impl.hbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HBaseEventSink implements EventSink {

    final Connection connection = ConnectionFactory.createConnection();

    public HBaseEventSink() throws IOException {
    }

    @Override
    public Long publishSync(String channelName, BsonObject event) {
        return null;
    }

    @Override
    public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
        return null;
    }

    @Override
    public void close() {

    }
}
