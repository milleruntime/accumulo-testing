/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing.core.randomwalk.replication;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.randomwalk.State;
import org.apache.accumulo.testing.core.randomwalk.Test;
import org.apache.accumulo.testing.core.randomwalk.RandWalkEnv;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class Verify extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    final String sourceTable = state.getString("source");
    final String destTable = state.getString("dest");
    int ROWS = state.getInteger("rows");
    int COLS = state.getInteger("cols");

    Scanner scanner = env.getAccumuloConnector().createScanner(sourceTable, Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text(""));
    int row = 0;
    int col = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(row, Integer.parseInt(entry.getKey().getRow().toString()));
      assertEquals(col, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
      col++;
      if (col == COLS) {
        row++;
        col = 0;
      }
    }
    if (row == 0) {
      log.debug("No data to replicate.");
      return;
    } else {
      log.debug("Found " + row + " rows in " + sourceTable);
    }

    // wait a little while for replication to take place
    sleepUninterruptibly(30, TimeUnit.SECONDS);

    // check the data
    scanner = env.getAccumuloConnector().createScanner(destTable, Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text(""));
    row = 0;
    col = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(row, Integer.parseInt(entry.getKey().getRow().toString()));
      assertEquals(col, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
      col++;
      if (col == COLS) {
        row++;
        col = 0;
      }
    }
    assertEquals(ROWS, row);
    assertEquals(0, col);
  }

  // junit isn't a dependency
  static void assertEquals(int expected, int actual) {
    if (expected != actual)
      throw new RuntimeException(String.format("%d fails to match expected value %d", actual, expected));
  }
}
