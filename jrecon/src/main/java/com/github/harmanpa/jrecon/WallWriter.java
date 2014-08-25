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
import com.github.harmanpa.jrecon.exceptions.WriteOnlyException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * This method is called when all tables and objects have been defined. Once
     * called, it is not possible to add new tables or objects. Furthermore, it
     * is not possible to add rows or fields until the wall has been finalized.
     *
     * @throws java.io.IOException
     */
    @Override
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
            buffer.putInteger(variableHeaderSize);
            // Buffer variable header
            buffer.put(bufferPacker.toByteArray());
            bufferPacker.clear();
            defined = true;
        }
    }

    @Override
    protected ReconTable createTable(String name, Iterable<String> signals) {
        return new WallTableWriter(name, signals);
    }

    @Override
    protected ReconObject createObject(String name) {
        return new WallObjectWriter(name);
    }

    class WallTableWriter extends ReconTableWriter {

        private final Set<Alias> aliases;

        public WallTableWriter(String name, Iterable<String> signals) {
            super(name, signals);
            this.aliases = Sets.newLinkedHashSet();
        }

        @Override
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
                buffer.putInteger(bufferPacker.getBufferSize());
                buffer.put(bufferPacker.toByteArray());
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new row", ex);
            }
        }

        @Override
        public final void addAlias(final String alias, String of, String transform) throws ReconException {
            checkNotFinalized();
            checkSignalExistence(of, true);
            checkSignalExistence(alias, false);
            if (!Sets.filter(aliases, new Predicate<Alias>() {
                @Override
                public boolean apply(Alias t) {
                    return t.getAlias().equals(alias);
                }
            }).isEmpty()) {
                throw new ReconException("Alias already exists");
            }
            Alias a = new Alias(alias, of, transform);
            aliases.add(a);
        }

        @Override
        public final Alias[] getAliases() {
            return Iterables.toArray(aliases, Alias.class);
        }

        @Override
        public final String[] getVariables() {
            List<String> vars = Lists.newArrayList();
            for (Alias alias : aliases) {
                vars.add(alias.getAlias());
            }
            vars.addAll(signals);
            return vars.toArray(new String[0]);
        }

        @Override
        public void setSignal(String signal, Object... data) throws ReconException {
            throw new TransposedException();
        }
    }

    /**
     * This class is used to write object fields back to a wall.
     */
    class WallObjectWriter extends ReconObjectWriter {

        public WallObjectWriter(String name) {
            super(name);
        }

        @Override
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
                buffer.putInteger(bufferPacker.getBufferSize());
                buffer.put(bufferPacker.toByteArray());
                bufferPacker.clear();
            } catch (IOException ex) {
                throw new ReconException("Error writing new field", ex);
            }
        }

        @Override
        public final Map<String, Object> getFields() throws ReconException {
            throw new WriteOnlyException();
        }
    }
}
