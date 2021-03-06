package io.mewbase.eventsource.impl.hbase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.util.FallibleFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


public class HBaseEventSource implements EventSource {

    private final static Logger log = LoggerFactory.getLogger(HBaseEventSource.class);

    final Connection connection = ConnectionFactory.createConnection();

    public HBaseEventSource() throws IOException {

    }

    @Override
    public CompletableFuture<Subscription> subscribe(String channelName, EventHandler eventHandler) {
        try {
            final Table table = ensureTable(channelName);
            final long startInclusive = getMostRecentEventNumber(table) + 1L;
            return new HBaseEventSubscription(table, startInclusive, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return FallibleFuture.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        try {
            final Table table = ensureTable(channelName);
            final long mren = getMostRecentEventNumber(table);
            final long startInclusive = Math.max(0L,mren);
            return new HBaseEventSubscription(table, startInclusive, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return FallibleFuture.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        try {
            final Table table = ensureTable(channelName);
            return new HBaseEventSubscription(table,startInclusive,eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return FallibleFuture.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        try {
            final Table table = ensureTable(channelName);
            final long mren = getTimestampEventNumber(table,startInstant.toEpochMilli());
            final long startInclusive = Math.max(0L,mren);
            return new HBaseEventSubscription(table, startInclusive, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return FallibleFuture.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribeAll(String channelName, EventHandler eventHandler) {
        return subscribeFromEventNumber(channelName, 0L,  eventHandler);
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
    private Table ensureTable(final String channelName ) throws Exception  {

        TableName tableName = TableName.valueOf(channelName);
        final Admin admin = connection.getAdmin();
        // Optimisation - Use local hash map to store "live" tables if the client API doesn't
        if (! admin.tableExists( tableName ) ) {
            createTable( tableName,  admin);
        }
        admin.close();
        Table table = connection.getTable(tableName);
        return table;
    }


    private void createTable(final TableName tableName, final Admin admin) throws IOException {
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor cfdb = ColumnFamilyDescriptorBuilder.of(HBaseEventSink.colFamily);
        tableBuilder.setColumnFamily(cfdb);
        admin.createTable( tableBuilder.build() );
    }


    private long getMostRecentEventNumber(final Table table) throws IOException  {
        Scan scan = new Scan();
        scan.setReversed(true);
        scan.setMaxResultSize(1); // i.e. search only the most recent item
        scan.addColumn(HBaseEventSink.colFamily, HBaseEventSink.qualifier);
        final ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        long mostRecentEventNumber = -1L;
        if ( result !=null && !result.isEmpty() ) {
            mostRecentEventNumber = Bytes.toLong(result.getRow());
        }
        log.info("Most recent Event for "+table.getName()+" is "+ mostRecentEventNumber);
        return mostRecentEventNumber;
    }


    private long getTimestampEventNumber(final Table table, long epochTimestampInclusive) throws IOException  {
        Scan scan = new Scan();
        scan.setTimeRange(epochTimestampInclusive, Long.MAX_VALUE);
        scan.setMaxResultSize(1); // i.e. search only for the nearest item.
        scan.addColumn(HBaseEventSink.colFamily, HBaseEventSink.qualifier);
        final ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        long timeStampedEventNumber = -1L;
        if ( result !=null && !result.isEmpty() ) {
            timeStampedEventNumber = Bytes.toLong(result.getRow());
        }
        log.info("Time Stamped Event for "+table.getName()+" is "+ timeStampedEventNumber);
        return timeStampedEventNumber;
    }


}
