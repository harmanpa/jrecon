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

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.io.File;
import java.io.IOException;
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
public class WallTest {

    @Test
    public void test0() {
        File f = new File(new File(System.getProperty("user.dir")), "src/test/resources/samples/test.wll");
        try {
            WallReader reader = new WallReader(f);
            for (ReconObject obj : reader.getObjects().values()) {
                for (Map.Entry<String, Object> entry : obj.getFields().entrySet()) {
                    System.out.println(obj.getName() + ": " + entry.getKey() + "=" + entry.getValue());
                }
            }
            for (ReconTable table : reader.getTables().values()) {
                for (String signal : table.getSignals()) {
                    System.out.println(table.getName() + ": " + signal + "=" + table.getSignal(signal).length);
                }
                for (Alias alias : table.getAliases()) {
                    String signal = alias.getAlias();
                    System.out.println(table.getName() + ": " + signal + "=" + table.getSignal(signal).length);
                }
            }
        } catch (ReconException | IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void test1() {
        try {
            File f = File.createTempFile("test", ".wll");
            // Create the wall object with a file object to write to
            WallWriter wall = new WallWriter(f);

            //Walls can contain tables, here is how we define one
            ReconTable t = wall.addTable("T1", new String[]{"time", "x", "y"});

            // Tables can also have aliases, here is how we define a few
            t.addAlias("a", "x");
            t.addAlias("b", "y");

            // Walls can also have objects.
            ReconObject obj1 = wall.addObject("obj1");
            ReconObject obj2 = wall.addObject("obj2");

            // Note, so far we have not specified the values of anything.
            // Once we "define" the wall, we can't change the structure,
            // but we can add rows to tables and fields to objects.
            wall.finalizeDefinitions();

            t.addRow(0.0, 1.0, 2.0);
            wall.flush(); // We can write data out at any time
            t.addRow(1.0, 0.0, 3.0);
            wall.flush();
            t.addRow(2.0, 1.0, 3.0);
            wall.flush();

            // Here we are adding fields to our object
            obj1.addField("name", "Mike");
            obj2.addField("name", "Pete");
            wall.flush();

            // Adding more fields
            obj1.addField("nationality", "American");
            obj2.addField("nationality", "UKLander");
            wall.flush();

            // Question, should we be allowed to overwrite fields?
            // If we are really journaling, this should be ok, e.g.,
            obj2.addField("nationality", "GreatBritisher");
            wall.flush();

            WallReader reader = new WallReader(f);
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

        } catch (ReconException | IOException ex) {
            Logger.getLogger(WallTest.class.getName()).log(Level.SEVERE, null, ex);
            Assert.fail();
        }
    }
}
