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

    public void finalizeDefinitions() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
