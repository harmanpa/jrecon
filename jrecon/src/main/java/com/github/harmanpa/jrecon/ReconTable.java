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
import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author pete
 */
public interface ReconTable extends Serializable {

    public String getName();

    public String[] getSignals();

    public Alias[] getAliases();
    
    public String[] getVariables();

    public Map<String, Object> getTableMeta();

    public Map<String, Object> getSignalMeta(String signal);

    public void addRow(Object... data) throws ReconException;

    public void addMeta(String name, Object data) throws ReconException;

    public void addSignalMeta(String signal, String name, Object data) throws ReconException;
    
    public void addSignal(String signal) throws ReconException;

    public void addAlias(String var, String alias) throws ReconException;

    public void addAlias(String var, String alias, String transform) throws ReconException;
    
    public void setSignal(String signal, Object... data) throws ReconException;
    
    public Object[] getSignal(String signal) throws ReconException;
    
    public <T> T[] getSignal(String signal, Class<T> c) throws ReconException;    
}
