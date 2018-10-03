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
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author pete
 */
public class Meld {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 2) {
            try {
                wallToMeld(new File(args[0]), new File(args[1]));
            } catch (IOException | ReconException ex) {
                Logger.getLogger(Meld.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void wallToMeld(File wall, File meld) throws IOException, ReconException {
        wallToMeld(wall, meld, false);
    }

    public static void wallToMeld(File wall, File meld, boolean compressed) throws IOException, ReconException {
        WallReader reader = new WallReader(wall);
        MeldWriter writer = new MeldWriter(meld, compressed);
        wall2meld(reader, writer);
    }

    public static void wall2meld(WallReader reader, MeldWriter writer) throws ReconException, IOException {
        // Add meta data
        for (Map.Entry<String, Object> entry : reader.getFileMeta().entrySet()) {
            writer.addMeta(entry.getKey(), entry.getValue());
        }
        // Iterate over tables, adding each table, it's signals and aliases
        for (ReconTable table : reader.getTables().values()) {
            ReconTable newTable = writer.addTable(table.getName(), table.getSignals());
            for (Alias alias : table.getAliases()) {
                newTable.addAlias(alias.getAlias(), alias.getOf(), alias.getTransform());
            }
            for (Map.Entry<String, Object> entry : table.getTableMeta().entrySet()) {
                newTable.addMeta(entry.getKey(), entry.getValue());
            }
            for (String signal : table.getSignals()) {
                for (Map.Entry<String, Object> entry : table.getSignalMeta(signal).entrySet()) {
                    newTable.addSignalMeta(signal, entry.getKey(), entry.getValue());
                }
            }
        }
        // Iterate over objects
        for (ReconObject object : reader.getObjects().values()) {
            ReconObject newObject = writer.addObject(object.getName());
            for (Map.Entry<String, Object> entry : object.getObjectMeta().entrySet()) {
                newObject.addMeta(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, Object> entry : object.getFields().entrySet()) {
                newObject.addField(entry.getKey(), entry.getValue());
            }
        }
        writer.finalizeDefinitions();
        writer.flush();
        // Iterate over rows, building table data
        for (ReconTable table : reader.getTables().values()) {
            ReconTable writerTable = writer.getTables().get(table.getName());
            for (String signal : table.getVariables()) {
                writerTable.setSignal(signal, table.getSignal(signal));
                writer.flush();
            }
        }
        // Write
        writer.close();
        reader.close();
    }

    public static void csv2wall(CSVParser reader, Function<String, String> headerExtractor, Function<String, Object> valueExtractor, WallWriter writer) throws ReconException, IOException {
        ReconTable table = writer.addTable("csv", Iterables.toArray(Iterables.transform(reader.getHeaderMap().keySet(), headerExtractor), String.class));
        writer.finalizeDefinitions();
        writer.flush();
        for (CSVRecord row : reader) {
            table.addRow(Iterators.toArray(Iterators.transform(row.iterator(), valueExtractor), Object.class));
            writer.flush();
        }
        writer.close();
    }

    public static void csv2meld(CSVParser reader, Function<String, String> headerExtractor, Function<String, Object> valueExtractor, MeldWriter writer) throws IOException, ReconException {
        ReconTable table = writer.addTable("csv", Iterables.toArray(Iterables.transform(reader.getHeaderMap().keySet(), headerExtractor), String.class));
        writer.finalizeDefinitions();
        writer.flush();
        List<CSVRecord> rows = reader.getRecords();
        for (String column : reader.getHeaderMap().keySet()) {
            List<Object> values = new ArrayList<>(rows.size());
            rows.stream().map((row) -> row.get(column)).forEachOrdered((entry) -> {
                values.add(valueExtractor.apply(entry));
            });
            table.setSignal(headerExtractor.apply(column), values.toArray());
            writer.flush();
        }
        writer.close();
    }

    public static Function<String, String> defaultHeaderExtractor() {
        return Functions.identity();
    }

    public static Function<String, Object> defaultValueExtractor() {
        return (String f) -> Double.valueOf(f);
    }
}
