package com.github.harmanpa.jrecon.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 *
 * @author pete
 */
public class Compression {

    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        OutputStream os = new BZip2CompressorOutputStream(baos);
        os.write(data);
        os.close();
        return baos.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException {
        InputStream is = new BZip2CompressorInputStream(new ByteArrayInputStream(data));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        int b;
        while ((b = is.read()) > -1) {
            baos.write(b);
        }
        is.close();
        return baos.toByteArray();
    }
}
