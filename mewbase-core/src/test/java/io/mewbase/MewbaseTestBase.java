package io.mewbase;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.ext.unit.junit.RepeatRule;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.scalatest.exceptions.TestFailedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;


/**
 * Created by tim on 14/10/16.
 */
public class MewbaseTestBase {


    public static final int PROJECTION_SETUP_MAX_TIMEOUT = 3;
    public static final int SUBSCRIPTION_SETUP_MAX_TIMEOUT = 2;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    protected Config createConfig() {
        final String testPath;
        try {
            testPath = testFolder.newFolder().getPath();
        } catch (IOException e) {
            throw new IllegalStateException("Could not create a directory for file binder", e);
        }
        final Properties properties = new Properties();
        properties.setProperty("mewbase.binders.files.store.basedir", Paths.get(testPath,"binders").toString() );
        properties.setProperty("mewbase.event.sink.file.basedir", Paths.get(testPath,"events").toString() );
        properties.setProperty("mewbase.event.source.file.basedir", Paths.get(testPath,"events").toString() );
        ConfigFactory.invalidateCaches();
        return ConfigFactory.load(ConfigFactory.parseProperties(properties));
    }

}
