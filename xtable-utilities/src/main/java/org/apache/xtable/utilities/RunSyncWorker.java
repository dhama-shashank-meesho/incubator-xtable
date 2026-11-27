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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import lombok.extern.log4j.Log4j2;

/**
 * Runs {@link RunSync} once per line read from standard input, in one JVM, so that a caller
 * converting many tables pays for the JVM, class loading and the Spark session once instead of once
 * per table.
 *
 * <p>Protocol, one line each:
 *
 * <ul>
 *   <li>on start, stdout: {@code XTABLE_WORKER_READY}
 *   <li>stdin: {@code <jobId>\t<RunSync argument>\t<RunSync argument>...}, for example {@code
 *       job-1\t--datasetConfig\t/app/config/job-1.yaml}
 *   <li>stdout: {@code XTABLE_WORKER_START <jobId>}, then the sync's own output
 *   <li>stdout: {@code XTABLE_WORKER_END <jobId> OK}, or {@code XTABLE_WORKER_END <jobId> FAILED
 *       <reason>} when RunSync threw; stderr: {@code XTABLE_WORKER_END <jobId>}, so a reader of
 *       either stream knows the job's output on it is complete
 * </ul>
 *
 * The worker exits when standard input closes. A sync failure inside RunSync is logged by RunSync
 * ("Error running sync ...") and still ends in OK, exactly as a separate RunSync process exits 0;
 * callers keep scanning the output for it. An {@link Error} (out of memory, linkage) ends the job
 * as FAILED and exits the JVM with status 1, since its state can no longer be trusted.
 */
@Log4j2
public class RunSyncWorker {
  static final String READY = "XTABLE_WORKER_READY";
  static final String START = "XTABLE_WORKER_START ";
  static final String END = "XTABLE_WORKER_END ";

  /** One sync, given RunSync's arguments. */
  interface Sync {
    void run(String[] args) throws Exception;
  }

  public static void main(String[] args) throws IOException {
    BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    int status = new RunSyncWorker().serve(in, System.out, System.err, RunSync::main);
    if (status != 0) {
      System.exit(status);
    }
  }

  /**
   * Serves jobs until {@code in} ends. Returns 0, or 1 after an {@link Error}, when it stops
   * without reading further.
   */
  int serve(BufferedReader in, PrintStream out, PrintStream err, Sync sync) throws IOException {
    out.println(READY);
    out.flush();
    String line;
    while ((line = in.readLine()) != null) {
      if (line.trim().isEmpty()) {
        continue;
      }
      String[] parts = line.split("\t", -1);
      String jobId = parts[0];
      String[] syncArgs = Arrays.copyOfRange(parts, 1, parts.length);
      out.println(START + jobId);
      out.flush();
      String result = "OK";
      Error fatal = null;
      try {
        sync.run(syncArgs);
      } catch (Exception e) {
        log.error("Worker job {} failed", jobId, e);
        result = "FAILED " + oneLine(e);
      } catch (Error e) {
        fatal = e;
        result = "FAILED " + oneLine(e);
      }
      if (fatal == null) {
        clearCaches();
      }
      out.println(END + jobId + " " + result);
      out.flush();
      err.println(END + jobId);
      err.flush();
      if (fatal != null) {
        fatal.printStackTrace(err);
        err.flush();
        return 1;
      }
    }
    return 0;
  }

  private static String oneLine(Throwable t) {
    String message = t.getMessage() == null ? "" : ": " + t.getMessage();
    return (t.getClass().getName() + message).replaceAll("\\s+", " ");
  }

  /**
   * Drops what one table's sync left cached in this JVM, so the next table does not carry it:
   * Delta's DeltaLog instances with their snapshots, and datasets cached in the Spark session. Then
   * asks for a collection, so the heap a large table needed can shrink back. Spark and Delta are
   * runtime dependencies of this module, so they are reached by reflection.
   */
  static void clearCaches() {
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaLog").getMethod("clearCache").invoke(null);
    } catch (Throwable t) {
      log.debug("Could not clear the DeltaLog cache", t);
    }
    try {
      Object session =
          Class.forName("org.apache.spark.sql.SparkSession")
              .getMethod("getDefaultSession")
              .invoke(null);
      if ((Boolean) session.getClass().getMethod("isDefined").invoke(session)) {
        Object spark = session.getClass().getMethod("get").invoke(session);
        Object catalog = spark.getClass().getMethod("catalog").invoke(spark);
        catalog.getClass().getMethod("clearCache").invoke(catalog);
      }
    } catch (Throwable t) {
      log.debug("Could not clear the Spark cache", t);
    }
    System.gc();
  }
}
