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
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class TestRunSyncWorker {
  private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
  private final List<List<String>> calls = new ArrayList<>();

  private int serve(String input, RunSyncWorker.Sync sync) throws IOException {
    return new RunSyncWorker()
        .serve(
            new BufferedReader(new StringReader(input)),
            new PrintStream(outBytes, true, "UTF-8"),
            new PrintStream(errBytes, true, "UTF-8"),
            sync);
  }

  private List<String> out() {
    return Arrays.asList(new String(outBytes.toByteArray(), StandardCharsets.UTF_8).split("\n"));
  }

  private String err() {
    return new String(errBytes.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  void runsEachLineAsOneSyncWithMarkers() throws IOException {
    int status =
        serve(
            "job-1\t--datasetConfig\t/tmp/a.yaml\n\njob-2\t--datasetConfig\t/tmp/b c.yaml\n",
            args -> calls.add(Arrays.asList(args)));

    assertEquals(0, status);
    assertEquals(
        Arrays.asList(
            Arrays.asList("--datasetConfig", "/tmp/a.yaml"),
            Arrays.asList("--datasetConfig", "/tmp/b c.yaml")),
        calls);
    assertEquals(
        Arrays.asList(
            "XTABLE_WORKER_READY",
            "XTABLE_WORKER_START job-1",
            "XTABLE_WORKER_END job-1 OK",
            "XTABLE_WORKER_START job-2",
            "XTABLE_WORKER_END job-2 OK"),
        out());
    assertEquals("XTABLE_WORKER_END job-1\nXTABLE_WORKER_END job-2\n", err());
  }

  @Test
  void exceptionFailsOnlyItsJob() throws IOException {
    int status =
        serve(
            "job-1\t--datasetConfig\t/tmp/a.yaml\njob-2\t--datasetConfig\t/tmp/b.yaml\n",
            args -> {
              calls.add(Arrays.asList(args));
              if (calls.size() == 1) {
                throw new IOException("no such\nfile");
              }
            });

    assertEquals(0, status);
    assertEquals(2, calls.size());
    assertEquals("XTABLE_WORKER_END job-1 FAILED java.io.IOException: no such file", out().get(2));
    assertEquals("XTABLE_WORKER_END job-2 OK", out().get(4));
  }

  @Test
  void errorEndsTheJobAndStopsTheWorker() throws IOException {
    int status =
        serve(
            "job-1\t--datasetConfig\t/tmp/a.yaml\njob-2\t--datasetConfig\t/tmp/b.yaml\n",
            args -> {
              calls.add(Arrays.asList(args));
              throw new OutOfMemoryError("Java heap space");
            });

    assertEquals(1, status);
    assertEquals(1, calls.size());
    assertEquals(
        "XTABLE_WORKER_END job-1 FAILED java.lang.OutOfMemoryError: Java heap space",
        out().get(out().size() - 1));
    assertTrue(err().startsWith("XTABLE_WORKER_END job-1\n"));
  }
}
