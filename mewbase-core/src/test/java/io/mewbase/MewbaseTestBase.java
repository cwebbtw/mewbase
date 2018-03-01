package io.mewbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.ext.unit.junit.RepeatRule;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Created by tim on 14/10/16.
 */
public class MewbaseTestBase {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public RepeatRule repeatRule = new RepeatRule();



    protected void waitUntil(BooleanSupplier supplier) {
        waitUntil(supplier, 10000);
    }

    protected void waitUntil(BooleanSupplier supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            if (supplier.getAsBoolean()) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new IllegalStateException("Timed out");
            }
        }
    }

    protected <T> T waitForNonNull(Supplier<T> supplier) {
        return waitForNonNull(supplier, 10000);
    }

    protected <T> T waitForNonNull(Supplier<T> supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            T res = supplier.get();
            if (res != null) {
                return res;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new IllegalStateException("Timed out");
            }
        }
    }

    private final Random random = new Random();

    protected int randomInt() {
        return random.nextInt();
    }

    protected long randomLong() {
        return random.nextLong();
    }

    protected String randomString() {
        return UUID.randomUUID().toString();
    }

    //
    protected synchronized Config createConfig() throws Exception {
        final String testPath = testFolder.newFolder().getPath();
        System.setProperty("mewbase.binders.files.store.basedir", Paths.get(testPath,"binders").toString() );
        System.setProperty("mewbase.event.sink.file.basedir", Paths.get(testPath,"events").toString() );
        System.setProperty("mewbase.event.source.file.basedir", Paths.get(testPath,"events").toString() );
        ConfigFactory.invalidateCaches();
        Config cfg = ConfigFactory.load();
        return cfg;
    }

}
