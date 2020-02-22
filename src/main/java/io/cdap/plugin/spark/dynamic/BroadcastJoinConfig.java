/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.spark.dynamic;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Config for broadcast join.
 */
public class BroadcastJoinConfig extends PluginConfig {
  @Macro
  @Description("Path to load the small dataset from")
  private String path;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter;

  @Description("Schema of small dataset")
  private String datasetSchema;

  @Description("Keys to join on")
  private String joinOn;

  @Nullable
  @Description("Type of join")
  private String joinType;

  public String getPath() {
    return path;
  }

  public String getDelimiter() {
    return delimiter == null ? "," : delimiter;
  }

  public String getJoinType() {
    return joinType == null ? "INNER" : joinType;
  }

  public Schema getSmallDatasetSchema() {
    List<Schema.Field> fields = new ArrayList<>();
    // key1 type1, key2 type2, ...
    for (String keyType : datasetSchema.split(",")) {
      keyType = keyType.trim();
      int idx = keyType.lastIndexOf(" ");
      String fieldName = keyType.substring(0, idx);
      String typeStr = keyType.substring(idx + 1);
      Schema.Type type = Schema.Type.valueOf(typeStr.toUpperCase());
      fields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(type))));
    }
    return Schema.recordOf("smallDataset", fields);
  }

  public List<String> getJoinKeys() {
    return Arrays.stream(joinOn.split(",")).collect(Collectors.toList());
  }
}
