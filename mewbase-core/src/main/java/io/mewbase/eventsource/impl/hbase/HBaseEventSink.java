package io.mewbase.eventsource.impl.hbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hbase.TableName.*;

public class HBaseEventSink implements EventSink {

    final Connection connection = ConnectionFactory.createConnection();
    final Admin admin = connection.getAdmin();

    final private TableName table = valueOf("EventTable");
    final private String colFamily = "ColFamily";
    // final HTable desc = new HTable(table);

    public HBaseEventSink() throws IOException {

        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(valueOf("EventTable"));

        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(colFamily);
        tableBuilder.setColumnFamily(cfd);

        admin.createTable(tableBuilder.build() );

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
