package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.io.IOException;
import java.util.Map;

/**
 *
 * @author pete
 */
public interface ReconFile {

    public ReconTable addTable(String name, String... signals) throws ReconException;

    public Map<String, ReconTable> getTables() throws ReconException;

    public ReconObject addObject(String name) throws ReconException;

    public Map<String, ReconObject> getObjects() throws ReconException;

    public void addMeta(String name, Object value) throws ReconException;

    public Map<String, Object> getFileMeta() throws ReconException;

    public void flush() throws IOException;

    public boolean isDefined();

    public void finalizeDefinitions() throws IOException;
}
