package io.mewbase.eventsource.impl.hbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


public class HBaseEventSink implements EventSink {

    private final static Logger log = LoggerFactory.getLogger(HBaseEventSink.class);

    final Connection connection = ConnectionFactory.createConnection();

    final byte[] colFamily = Bytes.toBytes("Events");
    final byte[] qualifier = Bytes.toBytes("event");

    final Map<String,AtomicLong> seqNums = new HashMap<>();

    public HBaseEventSink() throws IOException {

    }


    @Override
    public Long publishSync(String channelName, BsonObject event) {
        try {
            Table table = ensureTable(channelName);
            AtomicLong l = seqNums.computeIfAbsent(channelName, s -> new AtomicLong());
            Put put = new Put(Bytes.toBytes( l.getAndIncrement()) );
            put.addColumn(colFamily,qualifier,event.encode().getBytes());
            table.put(put);
        } catch (Exception exp) {
            log.error("failed to get table from connection");
        }
        return EventSink.SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER;
    }

    @Override
    public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
        return CompletableFuture.supplyAsync( () -> publishSync( channelName,  event));
    }


    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Check if the table exists and if not then create
     * @return
     */
    private Table ensureTable(final String channelName ) throws IOException  {

        TableName tableName = TableName.valueOf(channelName);
        final Admin admin = connection.getAdmin();
        // Todo - Use local hash map to store "live" tables if the client API doesnt
        if (! admin.tableExists( tableName ) ) {
            createTable( tableName,  admin);
        }
        admin.close();
        Table table = connection.getTable(tableName);
        return table;
    }


    private void createTable(final TableName tableName, final Admin admin) throws IOException {
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor cfdb = ColumnFamilyDescriptorBuilder.of("Event");
        tableBuilder.setColumnFamily(cfdb);
        admin.createTable( tableBuilder.build() );
    }

}
