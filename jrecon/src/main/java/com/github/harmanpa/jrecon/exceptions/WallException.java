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
public class WallException extends Exception {

    /**
     * Creates a new instance of <code>WallException</code> without detail
     * message.
     */
    public WallException() {
    }

    /**
     * Constructs an instance of <code>WallException</code> with the specified
     * detail message.
     *
     * @param msg the detail message.
     */
    public WallException(String msg) {
        super(msg);
    }
}
