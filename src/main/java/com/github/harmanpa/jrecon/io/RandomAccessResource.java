/*
 * The MIT License
 *
 * Copyright 2014 CyDesign Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.harmanpa.jrecon.io;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 * @author pete
 */
public interface RandomAccessResource extends Serializable {
    
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
