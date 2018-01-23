package io.mewbase.eventsource.impl.file;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;

import java.nio.file.Files;
import java.time.Instant;


class FileEvent implements Event {

    private final static Logger logger = LoggerFactory.getLogger(FileEvent.class);
    final File file;

    FileEvent(File file) {
        this.file = file;
    }

    @Override
    public BsonObject getBson() {
        try {
            byte[] rbc = Files.readAllBytes(file.toPath());
            return new BsonObject(rbc);
        } catch(Exception exp) {
           logger.error("File read failed",exp);
        }
        return null;
    }

    @Override
    public Instant getInstant()  { return Instant.ofEpochMilli(file.lastModified()); }

    @Override
    public Long getEventNumber() { return FileEventUtils.eventNumberFromPath(file.toPath()); }

    @Override
    // Todo
    public int getCrc32() { return 0; }


    @Override
    public String toString() {
        return "TimeStamp : " + this.getInstant() +
                "EventNumber : " + this.getEventNumber() +
                "PayLoad : " + this.getBson();
    }


}
