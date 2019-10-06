package com.xdh.app.exp;

/**
 * FIX nessage parsing exception
 * 
 * @author haihuang
 *
 */
public class FIXParsingException extends Exception {
	/**
	 * default serial version ID
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * constructor
	 * 
	 * @param errorMessage	error message
	 */
	public FIXParsingException(String errorMessage) {
		super(errorMessage);
	}
	
	/**
	 * constructor
	 * 
	 * @param message	error message
	 * @param cause	Throwable
	 */
    public FIXParsingException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * constructor
     * @param cause	Throwable
     */
    public FIXParsingException(Throwable cause) {
        super(cause);
    }
	
}
