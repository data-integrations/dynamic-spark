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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Config for broadcast join.
 */
public class BroadcastJoinConfig extends PluginConfig {

  @Macro
  @Description("Number of datasets to join")
  private Integer numJoins;

  @Macro
  @Description("Path to load the small dataset from")
  private String path;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter;

  @Macro
  @Description("Schema of small dataset")
  private String datasetSchema;

  @Macro
  @Description("Keys to join on")
  private String joinOn;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType;

  /*
      Hacks...

      Dataset 2
   */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path2;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter2;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema2;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn2;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType2;

  /*
      Dataset 3
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path3;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter3;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema3;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn3;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType3;

  /*
      Dataset 4
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path4;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter4;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema4;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn4;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType4;

  /*
      Dataset 5
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path5;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter5;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema5;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn5;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType5;

  /*
      Dataset 6
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path6;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter6;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema6;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn6;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType6;

  /*
      Dataset 7
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path7;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter7;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema7;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn7;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType7;

  /*
      Dataset 8
  */

  @Macro
  @Nullable
  @Description("Path to load the small dataset from")
  private String path8;

  @Macro
  @Nullable
  @Description("Delimiter used in the small dataset")
  private String delimiter8;

  @Macro
  @Nullable
  @Description("Schema of small dataset")
  private String datasetSchema8;

  @Macro
  @Nullable
  @Description("Keys to join on")
  private String joinOn8;

  @Macro
  @Nullable
  @Description("Type of join")
  private String joinType8;

  public List<DatasetJoinInfo> getDatasetsToJoin() {
    List<DatasetJoinInfo> datasetJoinInfos = new ArrayList<>(8);
    for (int i = 0; i < numJoins; i++) {
      datasetJoinInfos.add(getJoinInfo(i + 1));
    }
    return datasetJoinInfos;
  }

  /**
   * @return whether the output schema can be calculated without macro evaluation.
   */
  public boolean canCalculateOutputSchema() {
    for (int i = 0; i < numJoins; i++) {
      String schemaName = getPropertyName("datasetSchema", i);
      String joinOnName = getPropertyName("joinOn", i);
      if (containsMacro(schemaName) || containsMacro(joinOnName)) {
        return false;
      }
    }
    return true;
  }

  private DatasetJoinInfo getJoinInfo(int datasetNum) {
    Map<String, String> rawProperties = getProperties().getProperties();
    String pathNameStr = getPropertyName("path", datasetNum);
    String path = rawProperties.get(pathNameStr);
    if (!containsMacro(pathNameStr) && path == null) {
      throw new IllegalArgumentException("Path for Dataset " + datasetNum + " must be specified.");
    }

    String joinType = rawProperties.get(getPropertyName("joinType", datasetNum));
    joinType = joinType == null ? "INNER" : joinType.toUpperCase();
    String delimiter = rawProperties.get(getPropertyName("delimiter", datasetNum));
    delimiter = delimiter == null ? "," : delimiter;

    String schemaStr = rawProperties.get(getPropertyName("datasetSchema", datasetNum));
    if (schemaStr == null || schemaStr.isEmpty()) {
      throw new IllegalArgumentException("Schema for Dataset " + datasetNum + " must be specified.");
    }
    Schema schema = parseSchema(schemaStr);

    String joinOnStr = rawProperties.get(getPropertyName("joinOn", datasetNum));
    if (joinOnStr == null || joinOnStr.isEmpty()) {
      throw new IllegalArgumentException("Join keys for Dataset " + datasetNum + " must be specified.");
    }
    Set<String> joinKeys = parseJoinKeys(joinOnStr);

    return new DatasetJoinInfo(datasetNum, path, joinType, delimiter, joinKeys, schema);
  }

  // schema of form: name1 type1, name2 type2, ...
  private Schema parseSchema(String schemaStr) {
    List<Schema.Field> fields = new ArrayList<>();
    // key1 type1, key2 type2, ...
    for (String keyType : schemaStr.split(",")) {
      keyType = keyType.trim();
      int idx = keyType.lastIndexOf(" ");
      String fieldName = keyType.substring(0, idx);
      String typeStr = keyType.substring(idx + 1);
      Schema.Type type = Schema.Type.valueOf(typeStr.toUpperCase());
      fields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(type))));
    }
    return Schema.recordOf("smallDataset", fields);
  }

  private Set<String> parseJoinKeys(String joinOn) {
    return Arrays.stream(joinOn.split(",")).collect(Collectors.toSet());
  }

  private String getPropertyName(String baseName, int num) {
    return num == 1 ? baseName : baseName + num;
  }
}
