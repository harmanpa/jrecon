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
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.msgpack.MessagePack;

/**
 *
 * @author pete
 */
public abstract class ReconFile {

    private MessagePack mp;

    protected MessagePack getMessagePack() {
        if (mp == null) {
            mp = new MessagePack();
        }
        return mp;
    }

    public abstract ReconTable addTable(String name, String... signals) throws ReconException;

    public abstract Map<String, ReconTable> getTables() throws ReconException;

    public abstract ReconObject addObject(String name) throws ReconException;

    public abstract Map<String, ReconObject> getObjects() throws ReconException;

    public abstract void addMeta(String name, Object value) throws ReconException;

    public abstract Map<String, Object> getFileMeta() throws ReconException;

    public abstract void flush() throws IOException;

    public abstract boolean isDefined();

    public abstract void finalizeDefinitions() throws IOException;

    public final ReconTable findTable(String name) throws ReconException {
        for (Entry<String, ReconTable> entry : getTables().entrySet()) {
            if (name.equals(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    public final ReconTable findTableForSignal(String name) throws ReconException {
        for (Entry<String, ReconTable> entry : getTables().entrySet()) {
            for (String sig : entry.getValue().getSignals()) {
                if (name.equals(sig)) {
                    return entry.getValue();
                }
            }
            for (Alias alias : entry.getValue().getAliases()) {
                if (name.equals(alias.getAlias())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public final ReconObject findObject(String name) throws ReconException {
        for (Entry<String, ReconObject> entry : getObjects().entrySet()) {
            if (name.equals(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }
}
