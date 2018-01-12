package io.mewbase.eventsource.impl.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;

public interface FileUtils {

    static Path pathFromEventNumber(long eventNumber) {
        return Paths.get(String.format("%016d", eventNumber));
    }

    static long eventNumberFromPath(Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

    static Optional<Path> mostRecentPath(Path channelPath) throws IOException {
        return Files.list(channelPath)
                .filter(f -> !Files.isDirectory(f))
                .max(Comparator.comparingLong(f -> f.toFile().lastModified()));
    }



}
