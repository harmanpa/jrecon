
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.util.Map;

/**
 *
 * @author pete
 */
public interface ReconObject {

    public String getName();

    public Map<String, Object> getObjectMeta();
    
    public Map<String, Object> getFields() throws ReconException;

    public void addField(String name, Object value) throws ReconException;

    public void addMeta(String name, Object value) throws ReconException;
}
