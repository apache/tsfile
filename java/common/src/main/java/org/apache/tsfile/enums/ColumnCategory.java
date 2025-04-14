package org.apache.tsfile.enums;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum ColumnCategory {
  TAG,
  FIELD,
  ATTRIBUTE;

  public static List<ColumnCategory> nCopy(ColumnCategory type, int n) {
    List<ColumnCategory> result = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      result.add(type);
    }
    return result;
  }
}
