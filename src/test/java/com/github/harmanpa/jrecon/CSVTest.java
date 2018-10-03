/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.io.FileRandomAccessResource;
import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Test;

/**
 *
 * @author pete
 */
public class CSVTest {

    @Test
    public void csv2wall() {
        try {
            InputStream is = ClassLoader.getSystemResourceAsStream("Modelica.Mechanics.Rotational.Examples.CoupledClutches.txt");
            CSVParser parser = CSVFormat.newFormat('\t')
                    .withHeader()
                    .withIgnoreSurroundingSpaces()
                    .withIgnoreEmptyLines().parse(new InputStreamReader(is));

            File f = File.createTempFile("test", ".mld");
            // Create the wall object with a file object to write to
            MeldWriter meld = new MeldWriter(f);
            Meld.csv2meld(parser, (String f1) -> f1.substring(0, f1.lastIndexOf('[')).trim(), Meld.defaultValueExtractor(), meld);

            MeldReader reader = new MeldReader(new FileRandomAccessResource(f));
            for(ReconTable table : reader.getTables().values()) {
                for(String variable : table.getVariables()) {
                    System.out.println(variable + " (" + Integer.toString(table.getSignal(variable).length) + ")");
                }
            }
        } catch (IOException ex) {
            Assert.fail(ex.toString());
        } catch (ReconException ex) {
            Logger.getLogger(CSVTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
