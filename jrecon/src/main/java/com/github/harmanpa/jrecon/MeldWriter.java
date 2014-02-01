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
import com.github.harmanpa.jrecon.exceptions.TransposedException;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author pete
 */
public class MeldWriter extends ReconWriter {

    private static final String MELD_ID = "recon:meld:v01";
    private static final String H_METADATA = "fmeta";
    private static final String H_TABLES = "tabs";
    private static final String H_OBJECTS = "objs";
    private static final String H_COMP = "comp";
    private static final String T_INDICES = "toff";
    private static final String T_VARIABLES = "vars";
    private static final String T_METADATA = "tmeta";
    private static final String T_VMETADATA = "vmeta";
    private static final String V_INDEX = "i";
    private static final byte[] V_INDHOLD = new byte[]{0x00, 0x00, 0x00, 0x00};
    private static final String V_LENGTH = "l";
    private static final String V_TRANS = "t";
    private static final String A_OF = "s";
    private static final String O_METADATA = "ometa";

    public MeldWriter(File file) {
        super(file);
    }

    @Override
    protected ReconTable createTable(String name, String[] signals) {
        return new MeldTableWriter(name, signals);
    }

    @Override
    protected ReconObject createObject(String name) {
        return new MeldObjectWriter(name);
    }

    /**
     * This method is called when all tables and objects have been defined. Once
     * called, it is not possible to add new tables or objects. Furthermore, it
     * is not possible to add rows or fields until the wall has been finalized.
     *
     * @throws java.io.IOException
     */
    public final void finalizeDefinitions() throws IOException {
        if (!defined) {
            bufferPacker.writeMapBegin(3);
            // Write file meta
            bufferPacker.write(H_METADATA);
            bufferPacker.write(getFileMeta());
            // Write table definitions
            bufferPacker.write(H_TABLES);
            bufferPacker.writeMapBegin(getTables().size());
            for (ReconTable table : getTables().values()) {
                bufferPacker.write(table.getName());
                bufferPacker.writeMapBegin(4);
                bufferPacker.write(T_METADATA);
                bufferPacker.write(table.getTableMeta());
                bufferPacker.write("sigs");
                bufferPacker.writeArrayBegin(table.getSignals().length);
                for (String signal : table.getSignals()) {
                    bufferPacker.write(signal);
                }
                bufferPacker.writeArrayEnd();
                bufferPacker.write("als");
                bufferPacker.writeMapBegin(table.getAliases().length);
                for (Alias alias : table.getAliases()) {
                    bufferPacker.write(alias.getAlias());
                    bufferPacker.writeMapBegin(alias.getTransform().isEmpty() ? 1 : 2);
                    bufferPacker.write("s");
                    bufferPacker.write(alias.getOf());
                    if (!alias.getTransform().isEmpty()) {
                        bufferPacker.write("t");
                        bufferPacker.write(alias.getTransform());
                    }
                    bufferPacker.writeMapEnd();
                }
                bufferPacker.writeMapEnd();
                bufferPacker.write("vmeta");
                bufferPacker.writeMapBegin(table.getSignals().length);
                for (String s : table.getSignals()) {
                    bufferPacker.write(s);
                    bufferPacker.write(table.getSignalMeta(s));
                }
                bufferPacker.writeMapEnd();
                bufferPacker.writeMapEnd();
            }
            bufferPacker.writeMapEnd();
            // Write object definitions
            bufferPacker.write(H_OBJECTS);
            bufferPacker.writeMapBegin(getObjects().size());
            for (ReconObject object : getObjects().values()) {
                bufferPacker.write(object.getName());
                bufferPacker.write(object.getObjectMeta());
            }
            bufferPacker.writeMapEnd();
            bufferPacker.writeMapEnd();
            int variableHeaderSize = bufferPacker.getBufferSize();
            // Buffer fixed header
            buffer.put(MELD_ID.getBytes());
            buffer.putInteger(variableHeaderSize);
            // Buffer variable header
            buffer.put(bufferPacker.toByteArray());
            bufferPacker.clear();
            defined = true;
        }
    }

    class MeldTableWriter extends ReconTableWriter {

        private final Map<String, Object[]> signalData;

        public MeldTableWriter(String name, String[] signals) {
            super(name, signals);
            this.signalData = new HashMap<String, Object[]>();
        }

        public final void addRow(Object... data) throws ReconException {
            throw new TransposedException();
        }

        public void setSignal(String signal, Object... data) throws ReconException {
            checkNotFinalized();
            signalData.put(signal, data);
        }

    }

    class MeldObjectWriter extends ReconObjectWriter {

        private final Map<String, Object> fieldData;

        public MeldObjectWriter(String name) {
            super(name);
            this.fieldData = new HashMap<String, Object>();
        }

        public void addField(String name, Object value) throws ReconException {
            checkNotFinalized();
            fieldData.put(name, value);
        }

        public Map<String, Object> getFields() throws ReconException {
            return ImmutableMap.copyOf(fieldData);
        }

    }
}
