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
public class NotFinalizedException extends Exception {

    /**
     * Creates a new instance of <code>NotFinalizedException</code> without
     * detail message.
     */
    public NotFinalizedException() {
    }

    /**
     * Constructs an instance of <code>NotFinalizedException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public NotFinalizedException(String msg) {
        super(msg);
    }
}
