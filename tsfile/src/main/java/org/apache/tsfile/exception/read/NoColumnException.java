package org.apache.tsfile.exception.read;

public class NoColumnException extends ReadProcessException{

  public NoColumnException(String columnName) {
    super(String.format("No column: %s", columnName));
  }
}
