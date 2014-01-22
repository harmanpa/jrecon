/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.harmanpa.jrecon;

import com.github.harmanpa.jrecon.exceptions.ReconException;
import java.util.Map;

/**
 *
 * @author pete
 */
public interface ReconTable {

    public String getName();

    public String[] getSignals();

    public Alias[] getAliases();

    public Map<String, Object> getTableMeta();

    public Map<String, Object> getSignalMeta(String signal);

    public void addRow(Object... data) throws ReconException;

    public void addMeta(String name, Object data) throws ReconException;

    public void addSignalMeta(String signal, String name, Object data) throws ReconException;

    public void addAlias(String var, String alias) throws ReconException;

    public void addAlias(String var, String alias, String transform) throws ReconException;
    
    public void setSignal(String signal, Object... data) throws ReconException;
    
    public Object[] getSignal(String signal) throws ReconException;
}
