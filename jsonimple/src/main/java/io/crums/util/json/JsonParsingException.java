/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.json;

import io.crums.util.json.simple.JSONObject;

/**
 * Unchecked exception for illegal JSON input.
 */
@SuppressWarnings("serial")
public class JsonParsingException extends RuntimeException {
  
  /**
   * Constructs and returns a 3-line error message. The 3 lines are
   * <ul>
   * <li>{@code [cause.getMesssage()]}</li>
   * <li>{@code on parsing:}</li>
   * <li>{@code [jObj.toString()]}</li>
   * </ul>
   * The first line in the message is omitted if {@code cause}
   * has no message
   * 
   * @param jObj    the JSON parsed
   * @param cause   underlying cause
   */
  public static String newMessage(JSONObject jObj, Throwable cause) {
    String cm = cause.getMessage();
    String head = cm == null || cm.isBlank() ? "" : (cm + "\n");
    return head + "on parsing:\n" + jObj;
  }
  
  

  public JsonParsingException() {
  }

  public JsonParsingException(String s) {
    super(s);
  }

  public JsonParsingException(Throwable cause) {
    super(cause.getMessage(), cause);
  }
  
  /**
   * Constructs an instance with a reasonably detailed message.
   * 
   * @param cause the underlying cause (ideally not an instance of this class)
   * @param jObj  the JSON being parsed (stringified in message)
   * 
   * @see #newMessage(JSONObject, Throwable)
   */
  public JsonParsingException(Throwable cause, JSONObject jObj) {
    this(newMessage(jObj, cause), cause);
  }

  public JsonParsingException(String message, Throwable cause) {
    super(message, cause);
  }


  @Override
  public synchronized JsonParsingException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  
  

}
