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

import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId;
import static org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId.INTEGER;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/*
* This class is generated using freemarker and the FilterTemplate.ftl template.
*/
public abstract class IntegerFilter extends ValueFilter {

  protected IntegerFilter(int measurementIndex) {
    super(measurementIndex);
  }

  protected IntegerFilter(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  public boolean satisfyInteger(long time, int value){
    return valueSatisfy(value);
  }

  @Override
  public ClassSerializeId getClassSerializeId() {
    return INTEGER;
  }

  protected abstract boolean valueSatisfy(int value);

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
          satisfyInfo[i] = valueSatisfy(valueColumn.getInt(i));
        }
      }
    }
    return satisfyInfo;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
  }
}
