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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Set;

/**
 * Information about a single dataset join
 */
public class DatasetJoinInfo {
  private final int num;
  private final String path;
  private final String joinType;
  private final String delimiter;
  private final Set<String> joinOn;
  private final Schema schema;

  public DatasetJoinInfo(int num, String path, String joinType, String delimiter, Set<String> joinOn, Schema schema) {
    this.num = num;
    this.path = path;
    this.joinType = joinType;
    this.delimiter = delimiter;
    this.joinOn = joinOn;
    this.schema = schema;
  }

  public int getNum() {
    return num;
  }

  public String getPath() {
    return path;
  }

  public String getJoinType() {
    return joinType;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public Set<String> getJoinKeys() {
    return joinOn;
  }

  public Schema getSchema() {
    return schema;
  }
}
