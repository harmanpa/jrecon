package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
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
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;

/**
 *
 * @author pete
 */
public class WallReader extends ReconReader {

    private final InputStream stream;
    private Map<String, ReconTable> tables;
    private Map<String, ReconObject> objects;
    private Map<String, Object> meta;
    private List<Row> rows;

    public WallReader(File file) throws IOException {
        this(new FileInputStream(file));
    }

    public WallReader(InputStream stream) throws IOException {
        this.stream = stream;
        // Read the fixed header
        byte[] fixed = new byte[18];
        if (18 != this.stream.read(fixed)) {
            throw new IOException("Could not read fixed header");
        }
        if (!"recon:wall:v01".equals(new String(Arrays.copyOf(fixed, 14)))) {
            throw new IOException("Incorrect file type");
        }
        int variableHeaderSize = ByteBuffer.wrap(Arrays.copyOfRange(fixed, 14, 18)).getInt();
        // Read the variable header
        byte[] variableHeaderBytes = new byte[variableHeaderSize];
        if (variableHeaderSize != this.stream.read(variableHeaderBytes)) {
            throw new IOException("Could not read variable header");
        }
        BufferUnpacker unpacker = new MessagePack().createBufferUnpacker(variableHeaderBytes);
        visitFile(unpacker);
        unpacker.close();
    }

    private void visitFile(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            if ("fmeta".equals(name)) {
                this.meta = visitMetaMap(unpacker);
            } else if ("tabs".equals(name)) {
                this.tables = visitTableMap(unpacker);
            } else if ("objs".equals(name)) {
                this.objects = visitObjectMap(unpacker);
            }
        }
        unpacker.readMapEnd();
    }

    private Object readObject(Unpacker unpacker) throws IOException {
        Value value = unpacker.readValue();
        switch (value.getType()) {
            case ARRAY:
                return arrayToObject(value.asArrayValue());
            case BOOLEAN:
                return Boolean.valueOf(value.asBooleanValue().getBoolean());
            case FLOAT:
                return Double.valueOf(value.asFloatValue().getDouble());
            case INTEGER:
                return Integer.valueOf(value.asIntegerValue().getInt());
            case RAW:
                return new String(value.asRawValue().getByteArray());
            default:
                return null;
        }
    }

    private Object arrayToObject(ArrayValue array) {
        if (array.size() == 0) {
            return new Object[0];
        }
        switch (array.getElementArray()[0].getType()) {
            case BOOLEAN:
                Boolean[] out = new Boolean[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out[i] = Boolean.valueOf(array.getElementArray()[i].asBooleanValue().getBoolean());
                }
                return out;
            case FLOAT:
                Double[] out2 = new Double[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out2[i] = Double.valueOf(array.getElementArray()[i].asFloatValue().getDouble());
                }
                return out2;
            case INTEGER:
                Integer[] out3 = new Integer[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out3[i] = Integer.valueOf(array.getElementArray()[i].asIntegerValue().getInt());
                }
                return out3;
            case RAW:
                String[] out4 = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    out4[i] = String.valueOf(array.getElementArray()[i].asRawValue().getString());
                }
                return out4;
            default:
                return new Object[0];
        }
    }

    private Map<String, Object> visitMetaMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        Builder<String, Object> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            Object value = readObject(unpacker);
            builder.put(name, value);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    private Map<String, ReconTable> visitTableMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        Builder<String, ReconTable> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            ReconTable table = visitTable(name, unpacker);
            builder.put(name, table);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    private ReconTable visitTable(String name, Unpacker unpacker) throws IOException {
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
            }
        }
        unpacker.readMapEnd();
        return new WallTableReader(name, signals.toArray(new String[0]), aliases.toArray(new Alias[0]), tableMeta, signalMeta);
    }

    private Map<String, ReconObject> visitObjectMap(Unpacker unpacker) throws IOException {
        int mapLength = unpacker.readMapBegin();
        Builder<String, ReconObject> builder = ImmutableMap.builder();
        for (int i = 0; i < mapLength; i++) {
            String name = unpacker.readString();
            ReconObject object = visitObject(name, unpacker);
            builder.put(name, object);
        }
        unpacker.readMapEnd();
        return builder.build();
    }

    private ReconObject visitObject(String name, Unpacker unpacker) throws IOException {
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
                        rows.add(visitRow(new MessagePack().createBufferUnpacker(rowBytes)));
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

    public Map<String, ReconObject> getObjects() {
        return objects;
    }

    public Map<String, ReconTable> getTables() {
        return tables;
    }

    public Map<String, Object> getFileMeta() {
        return meta;
    }

    public void flush() throws IOException {
    }

    class WallObjectReader extends ReconObjectReader {

        public WallObjectReader(String name, Map<String, Object> meta) {
            super(name, meta);
        }

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

        public WallTableReader(String name, String[] signals, Alias[] aliases, Map<String, Object> meta, Map<String, Map<String, Object>> signalMeta) {
            super(name, signals, aliases, meta, signalMeta);
        }
        
        public Object[] getSignal(String signal) throws ReconException {
            return getSignal(signal, Object.class);
        }
        
        public <T> T[] getSignal(String signal, Class<T> c) throws ReconException {
            try {
                List<T> out = new ArrayList<T>(readRows().size());
                int index = getSignalIndex(signal);
                if (index < 0) {
                    throw new ReconException("Attempting to load non-existent signal");
                }
                String transform = getSignalTransform(signal);
                for (Row row : readRows()) {
                    if (getName().equals(row.getName())) {
                        // TODO: apply transform
                        out.add((T)row.getColumn(index));
                    }
                }
                return Iterables.toArray(out, c);
            } catch (IOException ex) {
                throw new ReconException("Failed to read signal " + signal, ex);
            }
        }

        private int getSignalIndex(String signal) {
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
