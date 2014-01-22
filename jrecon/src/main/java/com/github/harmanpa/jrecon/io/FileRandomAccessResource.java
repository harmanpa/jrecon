
package com.github.harmanpa.jrecon.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author pete
 */
public class FileRandomAccessResource implements RandomAccessResource {

    private final RandomAccessFile file;
    private final boolean canWrite;

    public FileRandomAccessResource(File file) throws FileNotFoundException {
        this.canWrite = file.canWrite();
        this.file = new RandomAccessFile(file, canWrite ? "rw" : "r");
    }

    public boolean canWrite() {
        return canWrite;
    }

    @Override
    public int read(long location, byte[] bytes) throws IOException {
        this.file.seek(location);
        return this.file.read(bytes);
    }

    @Override
    public void write(long location, byte[] bytes) throws IOException {
        this.file.seek(location);
        this.file.write(bytes);
    }

}
