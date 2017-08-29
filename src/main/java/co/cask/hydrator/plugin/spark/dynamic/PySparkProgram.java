/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.spark.dynamic;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link Spark} program plugin for running PySpark script.
 */
@Plugin(type = "sparkprogram")
@Name("PySparkProgram")
@Description("Executes user-provided PySpark program")
public final class PySparkProgram extends AbstractSpark {

  private final Config config;

  public PySparkProgram(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    // Just to validate the extra py files URIs
    getExtraPyFiles();
  }

  @Override
  protected void initialize() throws Exception {
    super.initialize();
    getContext().setPySparkScript(config.getPythonCode(), getExtraPyFiles());
  }

  private List<URI> getExtraPyFiles() {
    if (config.containsMacro("pyFiles") || config.getPyFiles() == null) {
      return Collections.emptyList();
    }

    String[] libraries = config.getPyFiles().split(",");
    List<URI> extraPyFiles = new ArrayList<>(libraries.length);
    for (String lib : libraries) {
      extraPyFiles.add(URI.create(lib));
    }
    return extraPyFiles;
  }

  /**
   * Plugin configuration class.
   */
  public static final class Config extends PluginConfig {

    @Description(
      "The source code of the PySpark program written in Python. " +
        "The content must be valid Python code.")
    @Macro
    private final String pythonCode;

    @Description(
      "Extra libraries for the PySpark program. " +
        "It is a ',' separated list of URI for the locations of extra .egg, .zip and .py libraries."
    )
    @Macro
    @Nullable
    private final String pyFiles;

    public Config(String pythonCode, String pyFiles) {
      this.pythonCode = pythonCode;
      this.pyFiles = pyFiles;
    }

    String getPythonCode() {
      return pythonCode;
    }

    String getPyFiles() {
      return pyFiles;
    }
  }
}
