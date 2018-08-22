package org.apache.accumulo.testing.core.performance.tests;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Report;
import org.apache.accumulo.testing.core.performance.SystemConfiguration;
import org.apache.accumulo.testing.core.performance.util.TestData;

public class CryptoOffPT implements PerformanceTest {

  private static final int NUM_ROWS = 1000;
  private static final int NUM_FAMS = 2;
  private static final int NUM_QUALS = 2;

  @Override
  public SystemConfiguration getConfiguration() {
    Map<String,String> siteCfg = new HashMap<>();
    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    String tableName = "crypto";
    env.getConnector().tableOperations().create(tableName);

    long t1 = System.currentTimeMillis();
    TestData.generate(env.getConnector(), tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);
    long t2 = System.currentTimeMillis();
    env.getConnector().tableOperations().compact(tableName, null, null, true, true);
    long t3 = System.currentTimeMillis();

    Report.Builder builder = Report.builder();
    LongSummaryStatistics stats = runScans(env, tableName);
    builder.info("scan_stats", stats, "Times in ms to scan all rows");
    builder.result("scan", stats.getAverage(), "Average time in ms to scan all rows");

    builder.id("crypto").description("Crypto Off performance");
    builder.info("write", NUM_ROWS * NUM_FAMS * NUM_QUALS, t2 - t1, "Data write rate entries/sec ");
    builder.info("compact", NUM_ROWS * NUM_FAMS * NUM_QUALS, t3 - t2, "Compact rate entries/sec ");

    builder.parameter("rows", NUM_ROWS, "Rows in test table");
    builder.parameter("familes", NUM_FAMS, "Families per row in test table");
    builder.parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table");

    return builder.build();
  }

  private LongSummaryStatistics runScans(Environment env, String tableName) throws TableNotFoundException {
    LongSummaryStatistics stats = new LongSummaryStatistics();
    // run a few to get java runtime going
    for (int i = 0; i < 5; i++) {
      scan(tableName, env.getConnector());
    }

    for (int i = 0; i < 50; i++) {
      System.out.println("calling scan " + i);
      stats.accept(scan(tableName, env.getConnector()));
    }
    return stats;
  }

  private static long scan(String tableName, Connector c) throws TableNotFoundException {

    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      for (Map.Entry<Key,Value> entry : scanner) {
        count++;
      }
    }
    return System.currentTimeMillis() - t1;
  }
}
