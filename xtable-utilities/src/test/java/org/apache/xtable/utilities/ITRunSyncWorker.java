/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;

class ITRunSyncWorker {

  @Test
  void syncsSeveralTablesInOneJvm(@TempDir Path tempDir) throws IOException {
    try (GenericTable first =
            TestJavaHudiTable.forStandardSchema(
                "first", tempDir.resolve("first"), null, HoodieTableType.COPY_ON_WRITE);
        GenericTable second =
            TestJavaHudiTable.forStandardSchema(
                "second", tempDir.resolve("second"), null, HoodieTableType.COPY_ON_WRITE)) {
      first.insertRows(20);
      second.insertRows(20);
      File firstConfig = writeConfigFile(tempDir, first, "first");
      File secondConfig = writeConfigFile(tempDir, second, "second");
      String input =
          "job-1\t--datasetConfig\t"
              + firstConfig.getPath()
              + "\njob-2\t--datasetConfig\t"
              + secondConfig.getPath()
              + "\n";
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      int status =
          new RunSyncWorker()
              .serve(
                  new BufferedReader(new StringReader(input)),
                  new PrintStream(out, true, "UTF-8"),
                  new PrintStream(new ByteArrayOutputStream(), true, "UTF-8"),
                  RunSync::main);

      assertEquals(0, status);
      String output = new String(out.toByteArray(), StandardCharsets.UTF_8);
      assertEquals(
          "XTABLE_WORKER_READY\n"
              + "XTABLE_WORKER_START job-1\nXTABLE_WORKER_END job-1 OK\n"
              + "XTABLE_WORKER_START job-2\nXTABLE_WORKER_END job-2 OK\n",
          output);
      assertTrue(numIcebergMetadataJsonFiles(first) > 0);
      assertTrue(numIcebergMetadataJsonFiles(second) > 0);
    }
  }

  private static File writeConfigFile(Path tempDir, GenericTable table, String tableName)
      throws IOException {
    RunSync.DatasetConfig config =
        RunSync.DatasetConfig.builder()
            .sourceFormat("HUDI")
            .targetFormats(Collections.singletonList("ICEBERG"))
            .datasets(
                Collections.singletonList(
                    RunSync.DatasetConfig.Table.builder()
                        .tableBasePath(table.getBasePath())
                        .tableName(tableName)
                        .build()))
            .build();
    File configFile = tempDir.resolve(tableName + "-config.yaml").toFile();
    RunSync.YAML_MAPPER.writeValue(configFile, config);
    return configFile;
  }

  private static long numIcebergMetadataJsonFiles(GenericTable table) throws IOException {
    Path metadata = Paths.get(URI.create(table.getBasePath() + "/metadata"));
    try (Stream<Path> files = Files.list(metadata)) {
      return files.filter(p -> p.toString().endsWith("metadata.json")).count();
    }
  }
}
