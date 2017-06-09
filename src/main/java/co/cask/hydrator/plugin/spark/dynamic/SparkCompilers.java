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


import co.cask.cdap.api.spark.dynamic.SparkCompiler;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.Function0;
import scala.Option$;
import scala.collection.JavaConversions;
import scala.reflect.io.VirtualDirectory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;
import scala.tools.nsc.Settings;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Helper class for Spark compilation.
 */
public final class SparkCompilers {

  private static final FilenameFilter JAR_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".jar");
    }
  };

  /**
   * Creates a {@link SparkInterpreter} using reflection. This is needed until CDAP-11812 is resolved.
   */
  @Nullable
  public static SparkInterpreter createInterpreter() {
    try {
      ClassLoader classLoader = SparkInterpreter.class.getClassLoader();
      Settings settings = (Settings) classLoader
        .loadClass("co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler")
        .getDeclaredMethod("setClassPath", Settings.class)
        .invoke(null, new Settings());

      Class<?> interpreterClass = classLoader
        .loadClass("co.cask.cdap.app.runtime.spark.dynamic.DefaultSparkInterpreter");

      // There should be a constructor
      Constructor<?>[] constructors = interpreterClass.getDeclaredConstructors();
      if (constructors.length != 1) {
        return null;
      }
      Constructor<?> constructor = constructors[0];

      // Create a empty URLAdder implementation using proxy, because that class is not available via cdap api
      InvocationHandler handler = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
          // no-op
          return null;
        }
      };
      Class<?> urlAdderClass = classLoader.loadClass("co.cask.cdap.app.runtime.spark.dynamic.URLAdder");
      Object urlAdder = Proxy.newProxyInstance(classLoader, new Class<?>[]{urlAdderClass}, handler);
      Function0<BoxedUnit> onCloseFunc = new AbstractFunction0<BoxedUnit>() {
        @Override
        public BoxedUnit apply() {
          return BoxedUnit.UNIT;
        }
      };

      // For Spark 1, the constructor has 4 parameters. For Spark 2, it has 3 parameters
      SparkInterpreter sparkInterpreter = null;
      if (constructor.getParameterTypes().length == 4) {
        // Spark 1
        VirtualDirectory virtualDirectory = new VirtualDirectory("memory", Option$.MODULE$.<VirtualDirectory>empty());
        sparkInterpreter = (SparkInterpreter) constructor.newInstance(settings, virtualDirectory,
                                                                      urlAdder, onCloseFunc);

      } else if (constructor.getParameterTypes().length == 3) {
        // spark 2
        sparkInterpreter = (SparkInterpreter) constructor.newInstance(settings, urlAdder, onCloseFunc);
      }

      return sparkInterpreter;
    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException
      | IllegalAccessException | InstantiationException e) {
      return null;
    }
  }

  /**
   * Adds dependency jars to the given {@link SparkCompiler}.
   *
   * @param tempDir temporary directory for storing downloaded file
   * @param compiler the {@link SparkCompiler} to have dependencies added to
   * @param dependencies a common separated list of file path.
   * @throws IOException if failed to copy remote files to local temp directory
   */
  public static void addDependencies(File tempDir, SparkCompiler compiler, String dependencies) throws IOException {
    Set<File> depFiles = new LinkedHashSet<>();
    for (String dependency : dependencies.split(",")) {
      dependency = dependency.trim();

      boolean wildcard = dependency.endsWith("/*");
      if (wildcard) {
        dependency = dependency.substring(0, dependency.length() - 2);
      }

      URI uri;
      try {
        uri = new URI(dependency);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid dependency path " + dependency, e);
      }

      Configuration hConf = new Configuration();
      String scheme = uri.getScheme();
      if (scheme != null && !"file".equals(scheme)) {
        // Remote file. Copy the file to local tmp dir
        Path path = new Path(uri);
        FileSystem fs = path.getFileSystem(hConf);
        if (wildcard) {
          RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
          while (iterator.hasNext()) {
            copyPathAndAdd(fs, iterator.next().getPath(), tempDir, depFiles);
          }
        } else {
          copyPathAndAdd(fs, path, tempDir, depFiles);
        }
      } else {
        File file = scheme == null ? new File(dependency) : new File(uri);

        if (wildcard) {
          File[] jarFiles = file.listFiles(JAR_FILE_FILTER);
          if (jarFiles != null) {
            Collections.addAll(depFiles, jarFiles);
          }
        } else {
          depFiles.add(file);
        }
      }
    }

    compiler.addDependencies(JavaConversions.asScalaBuffer(new ArrayList<>(depFiles)));
  }

  /**
   * Copies a file from remote {@link FileSystem} to local file and add the new {@link File} to the given
   * {@link Collection}.
   */
  private static void copyPathAndAdd(FileSystem fs, Path from, File dir, Collection<File> files) throws IOException {
    File targetFile = new File(dir, from.getName());
    if (targetFile.exists()) {
      return;
    }

    try (InputStream is = fs.open(from)) {
      Files.copy(is, targetFile.toPath());
    }
    files.add(targetFile);
  }

  private SparkCompilers() {
    // no-op
  }
}
