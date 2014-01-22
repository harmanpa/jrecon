
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.io.File;
import java.io.IOException;
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected ReconObject createObject(String name) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void flush() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void finalizeDefinitions() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    class MeldTableWriter extends ReconTableWriter {

        public MeldTableWriter(String name, String[] signals) {
            super(name, signals);
        }

        public void addRow(Object... data) throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public void setSignal(String signal, Object... data) throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public Object[] getSignal(String signal) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

    
    class MeldObjectWriter extends ReconObjectWriter {

        public MeldObjectWriter(String name) {
            super(name);
        }

        public void addField(String name, Object value) throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public Map<String, Object> getFields() throws ReconException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
