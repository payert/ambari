/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.metrics.core.timeline.aggregators.v2;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.ambari.metrics.core.timeline.aggregators.AbstractTimelineAggregator;
import org.apache.ambari.metrics.core.timeline.availability.AggregationTaskRunner;
import org.apache.ambari.metrics.core.timeline.availability.MetricCollectorHAController;
import org.apache.ambari.metrics.core.timeline.query.AggregateCondition;
import org.apache.ambari.metrics.core.timeline.query.Condition;
import org.apache.ambari.metrics.core.timeline.query.DefaultCondition;
import org.apache.ambari.metrics.core.timeline.query.EmptyCondition;
import org.apache.ambari.metrics.core.timeline.query.PhoenixTransactSQL;
import org.apache.hadoop.conf.Configuration;
import org.apache.ambari.metrics.core.timeline.PhoenixHBaseAccessor;

import static org.apache.ambari.metrics.core.timeline.query.PhoenixTransactSQL.GET_AGGREGATED_HOST_METRIC_GROUPBY_SQL;

public class TimelineMetricHostAggregator extends AbstractTimelineAggregator {

  public TimelineMetricHostAggregator(AggregationTaskRunner.AGGREGATOR_NAME aggregatorName,
                                      PhoenixHBaseAccessor hBaseAccessor,
                                      Configuration metricsConf,
                                      String checkpointLocation,
                                      Long sleepIntervalMillis,
                                      Integer checkpointCutOffMultiplier,
                                      String hostAggregatorDisabledParam,
                                      String tableName,
                                      String outputTableName,
                                      Long nativeTimeRangeDelay,
                                      MetricCollectorHAController haController) {
    super(aggregatorName, hBaseAccessor, metricsConf, checkpointLocation,
      sleepIntervalMillis, checkpointCutOffMultiplier, hostAggregatorDisabledParam,
      tableName, outputTableName, nativeTimeRangeDelay, haController);
  }

  @Override
  protected void aggregate(ResultSet rs, long startTime, long endTime) throws IOException, SQLException {

    LOG.info("Aggregated host metrics for " + outputTableName +
      ", with startTime = " + new Date(startTime) +
      ", endTime = " + new Date(endTime));
  }

  @Override
  protected Condition prepareMetricQueryCondition(long startTime, long endTime) {
    String query = String.format(
        "UPSERT INTO %s (UUID, SERVER_TIME, METRIC_SUM, METRIC_COUNT, METRIC_MAX, METRIC_MIN) " +
            "SELECT UUID, %s AS SERVER_TIME, SUM(METRIC_SUM), SUM(METRIC_COUNT), MAX(METRIC_MAX), MIN(METRIC_MIN) " +
            "FROM %s",
        outputTableName, endTime, tableName);

    AggregateCondition condition = new AggregateCondition(getDownsampledHostMetricSkipUUIDsClause(), startTime, endTime);
    condition.setStatement(query);
    condition.setUuidNotCondition(true);
    condition.setDoUpdate(true);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Condition: " + condition.toString());
    }

    return condition;
  }
}