package org.apache.tsfile.exception.write;

public class NoTableException extends WriteProcessException{

  public NoTableException(String tableName) {
    super(String.format("Table %s not found", tableName));
  }
}
