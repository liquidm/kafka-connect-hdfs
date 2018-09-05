/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs;

import org.apache.kafka.connect.connector.ConnectRecord;

import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.util.DataUtils;
import io.confluent.connect.storage.errors.PartitionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LiquidmTimestampExtractor implements TimestampExtractor {
  private static final Logger log = LoggerFactory.getLogger(LiquidmTimestampExtractor.class);

  @Override
  public void configure(Map<String, Object> config) {}

  @Override
  public Long extract(ConnectRecord<?> record) {
    Object value = record.value();
    Object timestampValue = DataUtils.getNestedFieldValue(value, "timestamp");
    if (timestampValue instanceof Number) {
      return Double.valueOf(((Number) timestampValue).doubleValue() * 1000).longValue();
    } else {
      log.error(
          "Expected timestamp field to be instance of Number, got {}", 
          timestampValue.getClass()
      );
      throw new PartitionException("Error extracting timestamp from");
    }
  }
}
