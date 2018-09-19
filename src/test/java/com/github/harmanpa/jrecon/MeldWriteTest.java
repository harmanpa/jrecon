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
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.io.FileRandomAccessResource;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pete
 */
public class MeldWriteTest {

    @Test
    public void test1() {
        try {
            File f = File.createTempFile("test", ".mld");
            // Create the wall object with a file object to write to
            MeldWriter meld = new MeldWriter(f);

            //Melds can contain tables, here is how we define one
            ReconTable t = meld.addTable("T1", new String[]{"time"});

            // Melds can also have objects.
            //ReconObject obj1 = meld.addObject("obj1");
            //ReconObject obj2 = meld.addObject("obj2");

            // Note, so far we have not specified the values of anything.
            // Once we "define" the wall, we can't change the structure,
            // but we can add rows to tables and fields to objects.

            // Here we are adding fields to our object
//            obj1.addField("name", "Mike");
//            obj2.addField("name", "Pete");
//
//            // Adding more fields
//            obj1.addField("nationality", "American");
//            obj2.addField("nationality", "UKLander");
            meld.finalizeDefinitions();
            t.setSignal("time", 1.0, 2.0, 3.0);
//            t.setSignal("x", new double[]{1.0, 2.0, 3.0});
//            t.setSignal("y", new double[]{1.0, 2.0, 3.0});


            meld.flush();

            MeldReader reader = new MeldReader(new FileRandomAccessResource(f));
            for (ReconObject obj : reader.getObjects().values()) {
                for (Map.Entry<String, Object> entry : obj.getFields().entrySet()) {
                    System.out.println(obj.getName() + "." + entry.getKey() + "=" + entry.getValue());
                }
            }
            for (ReconTable table : reader.getTables().values()) {
                for (String signal : table.getSignals()) {
                    System.out.println(table.getName() + "." + signal + "=" + Arrays.toString(table.getSignal(signal)));
                }
                for (Alias alias : table.getAliases()) {
                    String signal = alias.getAlias();
                    System.out.println(table.getName() + "." + signal + "=" + Arrays.toString(table.getSignal(signal)));
                }
            }

        } catch (Exception ex) {
            Logger.getLogger(MeldWriteTest.class.getName()).log(Level.SEVERE, null, ex);
            Assert.fail();
        }
    }
}
