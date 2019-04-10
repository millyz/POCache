/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A class that allows a DataNode to communicate information about the latency
 * of all blocks.
 *
 * The wire representation of this structure is a list of
 * BlockLatencyReportProto messages.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class BlockLatencyReports {
  @Nonnull
  private final Map<Long, Double> blocksLatency;

  public static final BlockLatencyReports EMPTY_REPORT =
      new BlockLatencyReports(ImmutableMap.of());

  private BlockLatencyReports(Map<Long, Double> blocksLatency) {
    this.blocksLatency = blocksLatency;
  }

  public static BlockLatencyReports create(
      @Nullable Map<Long, Double> blocksLatency) {
    if (blocksLatency == null || blocksLatency.isEmpty()) {
      return EMPTY_REPORT;
    }
    return new BlockLatencyReports(blocksLatency);
  }

  public Map<Long, Double> getBlocksLatency() {
    return this.blocksLatency;
  }
}
