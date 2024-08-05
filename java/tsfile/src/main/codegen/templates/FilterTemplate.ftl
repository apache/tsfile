<@pp.dropOutputFile />
<#list filters as filter>
  <#assign className = "${filter.javaBoxName}Filter">
  <@pp.changeOutputFile name="/org/apache/tsfile/read/filter/basic/${className}.java" />
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.read.filter.basic;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
<#if filter.dataType == "Binary">
import org.apache.tsfile.utils.Binary;
</#if>

import java.nio.ByteBuffer;

public abstract class ${className} extends ValueFilter {

  protected ${className}(int measurementIndex) {
    super(measurementIndex);
  }

  protected ${className}(ByteBuffer buffer) {
    super(buffer);
  }

  protected abstract boolean valueSatisfy(${filter.dataType} value);

  @Override
  public boolean[] satisfyTsBlock(boolean[] selection, TsBlock tsBlock) {
    Column valueColumn = tsBlock.getValueColumns()[measurementIndex];
    boolean[] satisfyInfo = new boolean[selection.length];
    System.arraycopy(selection, 0, satisfyInfo, 0, selection.length);
    for (int i = 0; i < selection.length; i++) {
      if (selection[i]) {
        if (valueColumn.isNull(i)) {
          // null not satisfy any filter, except IS NULL
          satisfyInfo[i] = false;
        } else {
          satisfyInfo[i] = valueSatisfy(valueColumn.get${filter.dataType?cap_first}(i));
        }
      }
    }
    return satisfyInfo;
  }
}
</#list>