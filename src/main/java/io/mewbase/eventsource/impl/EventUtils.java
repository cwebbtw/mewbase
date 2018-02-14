package io.mewbase.eventsource.impl;

import java.util.zip.CRC32;
import java.util.zip.Checksum;


public interface EventUtils {

    static long checksum(byte[] evt) {
        Checksum crc = new CRC32();
        crc.update(evt,0, evt.length);
        return crc.getValue();
    }

}
