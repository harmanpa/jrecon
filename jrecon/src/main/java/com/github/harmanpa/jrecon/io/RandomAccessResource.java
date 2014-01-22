package com.github.harmanpa.jrecon.io;

import java.io.IOException;

/**
 *
 * @author pete
 */
public interface RandomAccessResource {
    
    /**
     * Returns true if resource is writable
     * @return 
     */
    public boolean canWrite();

    /**
     * Reads bytes.length bytes into bytes, starting at location
     *
     * @param location
     * @param bytes
     * @return number of bytes read
     * @throws IOException
     */
    public int read(long location, byte[] bytes) throws IOException;

    /**
     * Writes bytes.length bytes from bytes, starting at location
     *
     * @param location
     * @param bytes
     * @throws IOException
     */
    public void write(long location, byte[] bytes) throws IOException;
}
