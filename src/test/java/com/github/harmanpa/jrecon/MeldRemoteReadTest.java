/*
 * The MIT License
 *
 * Copyright 2014 CyDesign Limited.
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
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.io.HttpRandomAccessResource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pete
 */
public class MeldRemoteReadTest {
    @Test
    public void test() {
        try {
            String url = "http://github.com/harmanpa/jrecon/blob/master/src/test/resources/samples/fullRobot.mld?raw=true";
            String signal = "axis1.gear.bearingFriction.flange_a.phi";
            MeldReader reader = new MeldReader(new HttpRandomAccessResource(new URI(url)));
            ReconTable table = reader.findTableForSignal(signal);
            @SuppressWarnings("MismatchedReadAndWriteOfArray")
            Double[] t = table.getSignal("Time", Double.class);
            Double[] x = table.getSignal(signal, Double.class);
            System.out.println(x.length);
        } catch (ReconException | URISyntaxException ex) {
            Logger.getLogger(MeldRemoteReadTest.class.getName()).log(Level.SEVERE, null, ex);
            Assert.fail(ex.toString());
        }
    }
}
