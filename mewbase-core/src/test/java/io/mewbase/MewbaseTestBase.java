package io.mewbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.ext.unit.junit.RepeatRule;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;


/**
 * Created by tim on 14/10/16.
 */
public class MewbaseTestBase {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public RepeatRule repeatRule = new RepeatRule();


    protected synchronized Config createConfig() throws Exception {
        final File testRootFolder = testFolder.newFolder();
        while (!testRootFolder.exists()) {
            Thread.sleep(10);
        }
        final String testPath = testFolder.newFolder().getPath();

        System.setProperty("mewbase.binders.files.store.basedir", Paths.get(testPath,"binders").toString() );
        System.setProperty("mewbase.event.sink.file.basedir", Paths.get(testPath,"events").toString() );
        System.setProperty("mewbase.event.source.file.basedir", Paths.get(testPath,"events").toString() );
        ConfigFactory.invalidateCaches();
        Config cfg = ConfigFactory.load();
        return cfg;
    }

}
