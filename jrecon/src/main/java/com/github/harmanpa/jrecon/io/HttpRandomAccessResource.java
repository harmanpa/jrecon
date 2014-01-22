
package com.github.harmanpa.jrecon.io;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 *
 * @author pete
 */
public class HttpRandomAccessResource implements RandomAccessResource {

    private final URI uri;

    public HttpRandomAccessResource(URI uri) {
        this.uri = uri;
    }

    public boolean canWrite() {
        return false;
    }

    public int read(long location, byte[] bytes) throws IOException {
        HttpGet get = new HttpGet(uri);
        get.addHeader("Range", "bytes=" + Long.toString(location) + "-" + Integer.toString((int) location + bytes.length - 1));
        HttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(get);
        if (response.getStatusLine().getStatusCode() == 206) {
            HttpEntity entity = response.getEntity();
            return ByteStreams.read(entity.getContent(), bytes, 0, bytes.length);
        }
        throw new IOException(response.getStatusLine().getReasonPhrase());
    }

    public void write(long location, byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Write not supported");
    }

}
