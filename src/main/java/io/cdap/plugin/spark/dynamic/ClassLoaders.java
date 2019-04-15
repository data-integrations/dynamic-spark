/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Utility class for workaround a classloader bug CDAP-12743.
 */
public final class ClassLoaders {

  /**
   * Populates the list of {@link URL} that this ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader.
   *
   * @param classLoader the {@link ClassLoader} for searching urls
   * @param childFirst if {@code true}, urls gathered from the lower classloader in the hierarchy will be
   *                   added before the higher one.
   * @param urls a {@link Collection} for storing the {@link URL}s
   * @return the same {@link Collection} passed from the parameter
   */
  public static <T extends Collection<? super URL>> T getClassLoaderURLs(ClassLoader classLoader,
                                                                         boolean childFirst, T urls) throws Exception {
    Deque<URLClassLoader> classLoaders = collectURLClassLoaders(classLoader, new LinkedList<URLClassLoader>());

    Iterator<URLClassLoader> iterator = childFirst ? classLoaders.iterator() : classLoaders.descendingIterator();
    while (iterator.hasNext()) {
      ClassLoader cl = iterator.next();
      for (URL url : ((URLClassLoader) cl).getURLs()) {
        if (urls.add(url) && (url.getProtocol().equals("file"))) {
          addClassPathFromJar(url, urls);
        }
      }
    }

    return urls;
  }

  /**
   * Collects {@link URLClassLoader} along the {@link ClassLoader} chain. This method recognizes
   * CombineClassLoader and will unfold the delegating classloaders inside it.
   * The order of insertion
   *
   * @param classLoader the classloader to start the search from
   * @param result a collection for storing the result
   * @param <T> type of the collection
   * @return the result collection
   */
  private static <T extends Collection<? super URLClassLoader>> T collectURLClassLoaders(ClassLoader classLoader,
                                                                                         T result) throws Exception {
    // Do BFS from the bottom of the ClassLoader chain
    Deque<ClassLoader> queue = new LinkedList<>();
    queue.add(classLoader);
    while (!queue.isEmpty()) {
      ClassLoader cl = queue.remove();

      // Although CombineClassLoader is a URLClassLoader, we always get the delegates instead
      // This is for making sure we can get the parent ClassLoaders of each of the underlying delegate
      // for the search.
      if (cl.getClass().getSimpleName().equals("CombineClassLoader")) {
        List<ClassLoader> delegates = (List<ClassLoader>) cl.getClass().getMethod("getDelegates").invoke(cl);
        ListIterator<ClassLoader> iterator = delegates.listIterator(delegates.size());
        // Use add first for delegates, which effectively is replacing the current classloader
        while (iterator.hasPrevious()) {
          queue.addFirst(iterator.previous());
        }
      } else if (cl instanceof URLClassLoader) {
        result.add((URLClassLoader) cl);
      }

      if (cl.getParent() != null) {
        queue.add(cl.getParent());
      }
    }

    return result;
  }

  /**
   * Extracts the Class-Path attributed from the given jar file and add those classpath urls.
   */
  private static <T extends Collection<? super URL>> void addClassPathFromJar(URL url, T urls) {
    try {
      File file = new File(url.toURI());
      try (JarFile jarFile = new JarFile(file)) {
        // Get the Class-Path attribute
        Manifest manifest = jarFile.getManifest();
        if (manifest == null) {
          return;
        }
        Object attr = manifest.getMainAttributes().get(Attributes.Name.CLASS_PATH);
        if (attr == null) {
          return;
        }

        // Add the URL
        for (String path : attr.toString().split(" ")) {
          path = path.trim();
          if (path.isEmpty()) {
            continue;
          }
          URI uri = new URI(path);
          if (uri.isAbsolute()) {
            urls.add(uri.toURL());
          } else {
            urls.add(new File(file.getParentFile(), path.replace('/', File.separatorChar)).toURI().toURL());
          }
        }
      }
    } catch (Exception e) {
      // Ignore, as there can be jar file that doesn't exist on the FS.
    }
  }
}
