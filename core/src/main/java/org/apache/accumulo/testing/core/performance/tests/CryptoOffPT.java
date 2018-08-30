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
import org.apache.accumulo.testing.core.performance.util.CryptoUtils;
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
    return CryptoUtils.testStuff(env, NUM_ROWS, NUM_FAMS, NUM_QUALS);
  }

}
