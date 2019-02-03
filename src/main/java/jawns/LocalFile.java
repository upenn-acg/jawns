package jawns;

import java.io.File;

/** Thin wrapper over java.io.File that throws if constructed with an unreadable path */
public class LocalFile extends File {
    public LocalFile(String filePath) {
        super(filePath);
        if (!canRead()) {
            throw new IllegalStateException("can't read file "+getPath());
        }
    }
}
