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
import com.github.harmanpa.jrecon.utils.Transforms;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

/**
 *
 * @author pete
 */
public class WallReader extends ReconReader {

    private final InputStream stream;
    private List<Row> rows;

    public WallReader(File file) throws IOException {
        this(new FileInputStream(file));
    }

    public WallReader(InputStream stream) throws IOException {
        this.stream = stream;
    }

    @Override
    protected final String getFileTypeString() {
        return "recon:wall:v01";
    }

    @Override
    protected final byte[] readFixedHeaderBytes() throws IOException {
        byte[] fixed = new byte[18];
        if (18 != this.stream.read(fixed)) {
            throw new IOException("Could not read fixed header");
        }
        return fixed;
    }

    @Override
    protected final byte[] readVariableHeaderBytes(int size) throws IOException {
        byte[] variableHeaderBytes = new byte[size];
        if (size != this.stream.read(variableHeaderBytes)) {
            throw new IOException("Could not read variable header");
        }
        return variableHeaderBytes;
    }

    @Override
    protected final ReconTable visitTable(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> tableMeta = new HashMap<String, Object>();
        Map<String, Map<String, Object>> signalMeta = new HashMap<String, Map<String, Object>>();
        List<String> signals = new ArrayList<String>();
        List<Alias> aliases = new ArrayList<Alias>();
        int mapLength = unpacker.readMapBegin();
        for (int i = 0; i < mapLength; i++) {
            String entryName = unpacker.readString();
            if ("tmeta".equals(entryName)) {
                tableMeta.putAll(visitMetaMap(unpacker));
            } else if ("sigs".equals(entryName)) {
                int nSignals = unpacker.readArrayBegin();
                for (int j = 0; j < nSignals; j++) {
                    signals.add(unpacker.readString());
                }
                unpacker.readArrayEnd();
            } else if ("als".equals(entryName)) {
                int nAliases = unpacker.readMapBegin();
                for (int j = 0; j < nAliases; j++) {
                    String alias = unpacker.readString();
                    String of = "";
                    String transform = "";
                    int nData = unpacker.readMapBegin();
                    for (int k = 0; k < nData; k++) {
                        String aliasData = unpacker.readString();
                        if ("s".equals(aliasData)) {
                            of = unpacker.readString();
                        } else if ("t".equals(aliasData)) {
                            transform = unpacker.readString();
                        }
                    }
                    unpacker.readMapEnd();
                    aliases.add(new Alias(alias, of, transform));
                }
                unpacker.readMapEnd();
            } else if ("vmeta".equals(entryName)) {
                int nSignals = unpacker.readMapBegin();
                for (int j = 0; j < nSignals; j++) {
                    String signal = unpacker.readString();
                    Map<String, Object> map = visitMetaMap(unpacker);
                    signalMeta.put(signal, map);
                }
                unpacker.readMapEnd();
            } else {
                throw new IOException("Unknown field " + entryName + " in defintion of table " + name);
            }
        }
        unpacker.readMapEnd();
        return new WallTableReader(name, signals.toArray(new String[0]), aliases.toArray(new Alias[0]), tableMeta, signalMeta);
    }

    @Override
    protected final ReconObject visitObject(String name, Unpacker unpacker) throws IOException {
        Map<String, Object> objectMeta = visitMetaMap(unpacker);
        return new WallObjectReader(name, objectMeta);
    }

    private List<Row> readRows() throws ReconException, IOException {
        if (rows == null) {
            rows = new ArrayList<Row>(1000);
            byte[] four = new byte[4];
            boolean complete = false;
            while (!complete) {
                switch (this.stream.read(four)) {
                    case -1:
                        complete = true;
                        break;
                    case 4:
                        int size = ByteBuffer.wrap(four).order(ByteOrder.BIG_ENDIAN).getInt();
                        byte[] rowBytes = new byte[size];
                        if (size != this.stream.read(rowBytes)) {
                            throw new ReconException("Failed to read size of next row");
                        }
                        rows.add(visitRow(getMessagePack().createBufferUnpacker(rowBytes)));
                        break;
                    default:
                        throw new ReconException("Failed to read size of next row");
                }
            }
        }
        return rows;
    }

    private Row visitRow(Unpacker unpacker) throws IOException, ReconException {
        Row row;
        unpacker.readMapBegin();
        String name = unpacker.readString();
        switch (unpacker.getNextType()) {
            case ARRAY:
                int n = unpacker.readArrayBegin();
                Object[] rowData = new Object[n];
                for (int i = 0; i < n; i++) {
                    rowData[i] = readObject(unpacker);
                }
                unpacker.readArrayEnd();
                row = new Row(name, rowData);
                break;
            case MAP:
                int nM = unpacker.readMapBegin();
                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                for (int i = 0; i < nM; i++) {
                    String field = unpacker.readString();
                    Object value = readObject(unpacker);
                    builder.put(field, value);
                }
                unpacker.readMapEnd();
                row = new Row(name, builder.build());
                break;
            default:
                throw new ReconException("Unknown format of row");
        }
        unpacker.readMapEnd();
        return row;
    }

    class WallObjectReader extends ReconObjectReader {

        public WallObjectReader(String name, Map<String, Object> meta) {
            super(name, meta);
        }

        @Override
        public Map<String, Object> getFields() throws ReconException {
            Map<String, Object> out = new HashMap<String, Object>();
            try {
                for (Row row : readRows()) {
                    if (getName().equals(row.getName())) {
                        out.putAll(row.getMap());
                    }
                }
                return out;
            } catch (IOException ex) {
                throw new ReconException("Failed to read object fields " + getName(), ex);
            }
        }
    }

    class WallTableReader extends ReconTableReader {

        private final Alias[] aliases;

        WallTableReader(String name, String[] signals, Alias[] aliases, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            super(name, signals, meta, signalMeta);
            this.aliases = aliases;
        }

        @Override
        public final Alias[] getAliases() {
            return aliases;
        }

        @Override
        public final String[] getVariables() {
            List<String> aliasnames = Lists.newArrayList();
            for (Alias alias : aliases) {
                aliasnames.add(alias.getAlias());
            }
            return ObjectArrays.concat(getSignals(), aliasnames.toArray(new String[0]), String.class);
        }

        @Override
        public Object[] getSignal(String signal) throws ReconException {
            return getSignal(signal, Object.class);
        }

        @Override
        public <T> T[] getSignal(String signal, Class<T> c) throws ReconException {
            int index = getSignalIndex(signal);
            return getSignal(index, c);
        }

        protected Object[] getSignal(int index) throws ReconException {
            return getSignal(index, Object.class);
        }

        protected <T> T[] getSignal(int index, Class<T> c) throws ReconException {
            if (index < 0) {
                throw new ReconException("Attempting to load non-existent signal");
            }
            String signal = getSignalName(index);
            try {
                List<T> out = new ArrayList<T>(readRows().size());
                String transform = getSignalTransform(signal);
                for (Row row : readRows()) {
                    if (getName().equals(row.getName())) {
                        out.add(Transforms.apply(c, (T) row.getColumn(index), transform));
                    }
                }
                return Iterables.toArray(out, c);
            } catch (IOException ex) {
                throw new ReconException("Failed to read signal " + signal, ex);
            }
        }

        protected int getSignalIndex(String signal) {
            for (int i = 0; i < getSignals().length; i++) {
                if (signal.equals(getSignals()[i])) {
                    return i;
                }
            }
            for (Alias alias : getAliases()) {
                if (signal.equals(alias.getAlias())) {
                    return getSignalIndex(alias.getOf());
                }
            }
            return -1;
        }

        private String getSignalTransform(String signal) {
            for (Alias alias : getAliases()) {
                if (signal.equals(alias.getAlias())) {
                    return alias.getTransform();
                }
            }
            return "";
        }

        private String getSignalName(int index) throws ReconException {
            if (index > getSignals().length) {
                throw new ReconException("Attempting to load non-existent signal");
            }
            return getSignals()[index];
        }
    }

    class Row {

        private final String name;
        private final Object[] data;
        private final Map<String, Object> map;

        public Row(String name, Object[] data) {
            this.name = name;
            this.data = data;
            this.map = null;
        }

        public Row(String name, Map<String, Object> map) {
            this.name = name;
            this.data = null;
            this.map = map;
        }

        public String getName() {
            return name;
        }

        public Object getColumn(int i) throws ReconException {
            if (data == null) {
                throw new ReconException("Attempting to get column of object row");
            }
            if (i < 0 || i >= data.length) {
                throw new ReconException("Attempting to get out-of-bounds column from row");
            }
            return data[i];
        }

        public Object getField(String name) throws ReconException {
            if (map == null) {
                throw new ReconException("Attempting to get field of table row");
            }
            return map.get(name);
        }

        public Map<String, Object> getMap() throws ReconException {
            if (map == null) {
                throw new ReconException("Attempting to get field of table row");
            }
            return map;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 11 * hash + (this.name != null ? this.name.hashCode() : 0);
            hash = 11 * hash + Arrays.deepHashCode(this.data);
            hash = 11 * hash + (this.map != null ? this.map.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Row other = (Row) obj;
            if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
                return false;
            }
            if (!Arrays.deepEquals(this.data, other.data)) {
                return false;
            }
            if (this.map != other.map && (this.map == null || !this.map.equals(other.map))) {
                return false;
            }
            return true;
        }
    }
}
