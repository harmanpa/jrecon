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
import com.github.harmanpa.jrecon.utils.ExpandableByteBuffer;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.msgpack.packer.BufferPacker;

/**
 *
 * @author pete
 */
public class MeldWriter extends ReconWriter {

    private static final String MELD_ID = "recon:meld:v01";

    public MeldWriter(File file) {
        super(file);
    }

    @Override
    protected ReconTable createTable(String name, Iterable<String> signals) {
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
    @Override
    public final void finalizeDefinitions() throws IOException {
        if (!defined) {
            int lengthDifference = 0;
            bufferPacker.writeMapBegin(3);
            // Write file meta
            bufferPacker.write("fmeta");
            bufferPacker.write(getFileMeta());
            // Write table definitions
            bufferPacker.write("tabs");
            bufferPacker.writeMapBegin(getTables().size());
            for (ReconTable table : getTables().values()) {
                bufferPacker.write(table.getName());
                bufferPacker.writeMapBegin(4);
                bufferPacker.write("tmeta");
                bufferPacker.write(table.getTableMeta());
                bufferPacker.write("vars");
                bufferPacker.writeArrayBegin(table.getSignals().length);
                for (String signal : table.getSignals()) {
                    bufferPacker.write(signal);
                }
                bufferPacker.writeArrayEnd();
                bufferPacker.write("toff");
                bufferPacker.writeMapBegin(table.getSignals().length);
                for (String signal : table.getSignals()) {
                    OffsetLength ol = ((MeldTableWriter) table).getSignalOffsetLength(signal);
                    String transform = ((MeldTableWriter) table).getSignalTransform(signal);
                    bufferPacker.write(signal);
                    bufferPacker.writeMapBegin(3);
                    bufferPacker.write("i");
                    lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getOffset());
                    bufferPacker.write("l");
                    lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getLength());
                    bufferPacker.write("t");
                    bufferPacker.write(transform);
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
            bufferPacker.write("objs");
            bufferPacker.writeMapBegin(getObjects().size());
            for (ReconObject object : getObjects().values()) {
                OffsetLength ol = ((MeldObjectWriter) object).getOffsetLength();
                bufferPacker.write(object.getName());
                bufferPacker.writeMapBegin(3);
                bufferPacker.write("ometa");
                bufferPacker.write(object.getObjectMeta());
                bufferPacker.write("i");
                lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getOffset());
                bufferPacker.write("l");
                lengthDifference += writeIntegerByteDifference(bufferPacker, ol.getLength());
                bufferPacker.writeMapEnd();
            }
            bufferPacker.writeMapEnd();
            bufferPacker.writeMapEnd();
            int variableHeaderSize = bufferPacker.getBufferSize();
            // Buffer fixed header
            buffer.put(MELD_ID.getBytes());
            buffer.putInteger(variableHeaderSize);
            // Buffer variable header
            buffer.put(bufferPacker.toByteArray());
            pad(buffer, lengthDifference);
            bufferPacker.clear();
            defined = true;
        }
    }

    private void pad(ExpandableByteBuffer buffer, int n) {
        for (int i = 0; i < n; i++) {
            buffer.put((byte) 0x00);
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    private int writeIntegerByteDifference(BufferPacker packer, int value) throws IOException {
        int start = packer.getBufferSize();
        packer.write(value);
        return 4 - (packer.getBufferSize() - start);
    }

    class MeldTableWriter extends ReconTableWriter {

        private final Map<String, OffsetLength> offsetLengths;
        private final Map<String, String> transforms;

        public MeldTableWriter(String name, Iterable<String> signals) {
            super(name, signals);
            this.offsetLengths = new HashMap<String, OffsetLength>();
            this.transforms = new HashMap<String, String>();
        }

        @Override
        public final void addRow(Object... data) throws ReconException {
            throw new TransposedException();
        }

        @Override
        public final void addAlias(final String alias, String of, String transform) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(of, true);
            checkSignalExistence(alias, false);
            addSignal(alias);
            transforms.put(alias, transform);
        }

        @Override
        public void setSignal(String signal, Object... data) throws ReconException {
            checkFinalized();
            //todo
        }

        @Override
        public Alias[] getAliases() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String[] getVariables() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        protected OffsetLength getSignalOffsetLength(String signal) {
            if (offsetLengths.containsKey(signal)) {
                return offsetLengths.get(signal);
            }
            return new OffsetLength(0, 0);
        }

        protected String getSignalTransform(String signal) {
            if (transforms.containsKey(signal)) {
                return transforms.get(signal);
            }
            return "";
        }
    }

    class MeldObjectWriter extends ReconObjectWriter {

        private final Map<String, Object> fieldData;
        private OffsetLength ol;

        public MeldObjectWriter(String name) {
            super(name);
            this.fieldData = new HashMap<String, Object>();
        }

        @Override
        public void addField(String name, Object value) throws ReconException {
            checkNotFinalized();
            fieldData.put(name, value);
        }

        @Override
        public Map<String, Object> getFields() throws ReconException {
            return ImmutableMap.copyOf(fieldData);
        }

        protected void writeData() {
            //TODO
        }

        protected OffsetLength getOffsetLength() {
            if (ol == null) {
                return new OffsetLength(0, 0);
            }
            return ol;
        }
    }
}
