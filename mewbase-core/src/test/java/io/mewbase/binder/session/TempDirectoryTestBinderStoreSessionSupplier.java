package io.mewbase.binder.session;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.filestore.FileBinderStore;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Supplier;

public class TempDirectoryTestBinderStoreSessionSupplier implements Supplier<TestBinderStoreSession> {

    @Override
    public TestBinderStoreSession get() {
        final Path tempDirectory;
        try {
            tempDirectory = Files.createTempDirectory("mewbase");
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        final Properties properties = new Properties();
        properties.setProperty("mewbase.binders.files.store.basedir", tempDirectory.toString());

        final Config config = ConfigFactory.load(ConfigFactory.parseProperties(properties));

        return new TestBinderStoreSession() {
            @Override
            public void close() throws Exception {
                FileUtils.deleteDirectory(tempDirectory.toFile());
            }

            @Override
            public BinderStore get() {
                return new FileBinderStore(config);
            }
        };
    }

}
