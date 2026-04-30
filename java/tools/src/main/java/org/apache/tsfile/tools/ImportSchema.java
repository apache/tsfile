/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ImportSchema {

  private String tableName = "";
  private String timePrecision = "ms";
  private boolean hasHeader = true;
  private String separator = ",";
  private String nullFormat;
  private String timeColumnName = "";
  private List<TagColumn> tagColumns = new ArrayList<>();
  private List<SourceColumn> sourceColumns = new ArrayList<>();

  public static class TagColumn {
    private final String name;
    private final boolean hasDefault;
    private final String defaultValue;

    public TagColumn(String name) {
      this.name = name;
      this.hasDefault = false;
      this.defaultValue = null;
    }

    public TagColumn(String name, String defaultValue) {
      this.name = name;
      this.hasDefault = true;
      this.defaultValue = defaultValue;
    }

    public String getName() {
      return name;
    }

    public boolean hasDefault() {
      return hasDefault;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public boolean isVirtual() {
      return hasDefault;
    }

    @Override
    public String toString() {
      if (hasDefault) {
        return "TagColumn{name='" + name + "', default='" + defaultValue + "'}";
      }
      return "TagColumn{name='" + name + "'}";
    }
  }

  public static class SourceColumn {
    private final String name;
    private final TSDataType dataType;
    private final boolean skip;

    public SourceColumn(String name, TSDataType dataType) {
      this.name = name;
      this.dataType = dataType;
      this.skip = false;
    }

    private SourceColumn() {
      this.name = null;
      this.dataType = null;
      this.skip = true;
    }

    public static SourceColumn skip() {
      return new SourceColumn();
    }

    public static SourceColumn skip(String name) {
      return new SourceColumn(name, true);
    }

    private SourceColumn(String name, boolean skip) {
      this.name = name;
      this.dataType = null;
      this.skip = skip;
    }

    public String getName() {
      return name;
    }

    public TSDataType getDataType() {
      return dataType;
    }

    public boolean isSkip() {
      return skip;
    }

    @Override
    public String toString() {
      if (skip) {
        return name != null ? "SourceColumn{SKIP, name='" + name + "'}" : "SourceColumn{SKIP}";
      }
      return "SourceColumn{name='" + name + "', type=" + dataType + "}";
    }
  }

  public List<SourceColumn> fieldColumns() {
    Set<String> tagNames = new HashSet<>();
    for (TagColumn tag : tagColumns) {
      if (!tag.isVirtual()) {
        tagNames.add(tag.getName());
      }
    }

    List<SourceColumn> fields = new ArrayList<>();
    for (SourceColumn col : sourceColumns) {
      if (col.isSkip()) {
        continue;
      }
      if (col.getName().equals(timeColumnName)) {
        continue;
      }
      if (tagNames.contains(col.getName())) {
        continue;
      }
      fields.add(col);
    }
    return Collections.unmodifiableList(fields);
  }

  // --- getters and setters ---

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTimePrecision() {
    return timePrecision;
  }

  public void setTimePrecision(String timePrecision) {
    this.timePrecision = timePrecision;
  }

  public boolean isHasHeader() {
    return hasHeader;
  }

  public void setHasHeader(boolean hasHeader) {
    this.hasHeader = hasHeader;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getNullFormat() {
    return nullFormat;
  }

  public void setNullFormat(String nullFormat) {
    this.nullFormat = nullFormat;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public void setTimeColumnName(String timeColumnName) {
    this.timeColumnName = timeColumnName;
  }

  public List<TagColumn> getTagColumns() {
    return tagColumns;
  }

  public void setTagColumns(List<TagColumn> tagColumns) {
    this.tagColumns = tagColumns;
  }

  public List<SourceColumn> getSourceColumns() {
    return sourceColumns;
  }

  public void setSourceColumns(List<SourceColumn> sourceColumns) {
    this.sourceColumns = sourceColumns;
  }

  @Override
  public String toString() {
    return "ImportSchema{"
        + "tableName='"
        + tableName
        + "', timePrecision='"
        + timePrecision
        + "', hasHeader="
        + hasHeader
        + ", separator='"
        + separator
        + "', nullFormat='"
        + nullFormat
        + "', timeColumnName='"
        + timeColumnName
        + "', tagColumns="
        + tagColumns
        + ", sourceColumns="
        + sourceColumns
        + ", fieldColumns="
        + fieldColumns()
        + '}';
  }
}
