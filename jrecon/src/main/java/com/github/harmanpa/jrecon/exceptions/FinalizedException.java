/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.github.harmanpa.jrecon.exceptions;

/**
 *
 * @author pete
 */
public class FinalizedException extends ReconException {

    /**
     * Creates a new instance of <code>FinalizedException</code> without detail
     * message.
     */
    public FinalizedException() {
    }

    /**
     * Constructs an instance of <code>FinalizedException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public FinalizedException(String msg) {
        super(msg);
    }
}
