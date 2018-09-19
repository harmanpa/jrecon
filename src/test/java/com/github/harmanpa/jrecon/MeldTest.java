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
import com.github.harmanpa.jrecon.io.FileRandomAccessResource;
import java.io.File;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pete
 */
public class MeldTest {

    @Test
    public void test() {
        try {
            File f = new File(new File(System.getProperty("user.dir")), "src/test/resources/samples/test.wll");
            File f2 = new File("target/test.mld");
            if (f2.exists()) {
                f2.delete();
            }
            f2.createNewFile();
            Meld.wallToMeld(f, f2);
            MeldReader reader = new MeldReader(new FileRandomAccessResource(f2));
            for(ReconTable table : reader.getTables().values()) {
                for(String signal : table.getSignals()) {
                    System.out.println(table.getName() + ":" + signal + "(" + Integer.toString(table.getSignal(signal).length) + ")");
                }
            }
        } catch (ReconException ex) {
            Assert.fail(ex.getMessage());
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
