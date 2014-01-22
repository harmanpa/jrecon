package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import com.github.harmanpa.jrecon.exceptions.TransposedException;
import com.github.harmanpa.jrecon.exceptions.WriteOnlyException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * This class is responsible for writing wall files.
 *
 * @author pete
 */
public class WallWriter extends ReconWriter {

    /**
     * This is a unique ID that every wall file starts with so it can be
     * identified/verified.
     */
    private static final String WALL_ID = "recon:wall:v01";

    public WallWriter(File file) {
        super(file);
    }

    /**
     * This flushes any pending rows of fields.
     *
     * @throws IOException
     */
    public final void flush() throws IOException {
        FileChannel channel = new FileOutputStream(file, defined).getChannel();
        buffer.flip();
        channel.write(buffer);
        buffer.compact();
        channel.close();
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
            bufferPacker.write("objs");
            bufferPacker.writeMapBegin(getObjects().size());
            for (ReconObject object : getObjects().values()) {
                bufferPacker.write(object.getName());
                bufferPacker.write(object.getObjectMeta());
            }
            bufferPacker.writeMapEnd();
            bufferPacker.writeMapEnd();
            int variableHeaderSize = bufferPacker.getBufferSize();
            // Buffer fixed header
            buffer.put(WALL_ID.getBytes());
            buffer.putInt(variableHeaderSize);
            // Buffer variable header
            buffer.put(bufferPacker.toByteArray());
            bufferPacker.clear();
            defined = true;
        }
    }

    @Override
    protected ReconTable createTable(String name, String[] signals) {
        return new WallTableWriter(name, signals);
    }

    @Override
    protected ReconObject createObject(String name) {
        return new WallObjectWriter(name);
    }


    class WallTableWriter extends ReconTableWriter {

        public WallTableWriter(String name, String[] signals) {
            super(name, signals);
        }

        public void addRow(Object... data) throws ReconException {
            checkFinalized();
            if (data.length != getSignals().length) {
                throw new ReconException("Number of data elements must match number of signals");
            }
            try {
                bufferPacker.writeMapBegin(1);
                bufferPacker.write(getName());
                bufferPacker.writeArrayBegin(data.length);
                for (Object obj : data) {
                    bufferPacker.write(obj);
                }
                bufferPacker.writeArrayEnd();
                bufferPacker.writeMapEnd();
                buffer.putInt(bufferPacker.getBufferSize());
                buffer.put(bufferPacker.toByteArray());
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new row", ex);
            }
        }

        public void setSignal(String signal, Object... data) throws ReconException {
            throw new TransposedException();
        }

        public Object[] getSignal(String signal) throws ReconException {
            throw new WriteOnlyException();
        }

    }

    /**
     * This class is used to write object fields back to a wall.
     */
    class WallObjectWriter extends ReconObjectWriter {

        public WallObjectWriter(String name) {
            super(name);
        }
        
        public final void addField(String name, Object value) throws ReconException {
            checkFinalized();
            try {
                bufferPacker.writeMapBegin(1);
                bufferPacker.write(getName());
                bufferPacker.writeMapBegin(1);
                bufferPacker.write(name);
                bufferPacker.write(value);
                bufferPacker.writeMapEnd();
                bufferPacker.writeMapEnd();
                buffer.putInt(bufferPacker.getBufferSize());
                buffer.put(bufferPacker.toByteArray());
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new field", ex);
            }
        }

        public final Map<String, Object> getFields() throws ReconException {
            throw new WriteOnlyException();
        }
    }
}
