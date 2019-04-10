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

/* POCache added
 */
package org.apache.hadoop.hdfs.server.protocol;

import com.google.common.base.Joiner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.util.Arrays;
import java.util.Collection;

/**
 * A BlockPCUpdateCommand is an instruction to a DataNode to
 * update the cached parity of a file.
 *
 * Upon receiving this command, the DataNode pulls data from other DataNodes
 * hosting blocks of this file and update the cached parity through codec
 * calculation.
 * After the update, the DataNode pushed the updated parity blocks
 * the memory store if necessary.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPCUpdateCommand extends DatanodeCommand {
  private final Collection<BlockPCUpdateInfo> pcTasks;

  /**
   * Create BlockPCUpdateCommand from a collection of
   * {@link BlockPCUpdateInfo}, each representing a update
   * task
   */
  public BlockPCUpdateCommand(int action,
      Collection<BlockPCUpdateInfo> blockPCUpdateInfoList) {
    super(action);
    this.pcTasks = blockPCUpdateInfoList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockPCUpdateCommand(\n ");
    Joiner.on("\n ").appendTo(sb, pcTasks);
    sb.append("\n)");
    return sb.toString();
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockPCUpdateInfo {
    private long fileId;
    private int newDatNumPcache;
    private int newParNumPcache;

    public BlockPCUpdateInfo(long fileId, int newDatNumPcache, int newParNumPcache) {
      this.fileId = fileId;
      this.newDatNumPcache = newDatNumPcache;
      this.newParNumPcache = newParNumPcache;
    }

    public long getFileId() { return fileId; }
    public int getNewDatNumPcache() { return newDatNumPcache; }
    public int getNewParNumPcache() { return newParNumPcache; }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockPCUpdateInfo(\n")
        .append("Update ").append(Long.toString(fileId))
        .append("To: (").append(newDatNumPcache).append(",").append(newParNumPcache)
        .append(")\n")
        .toString();
    }
  }

  public Collection<BlockPCUpdateInfo> getPCTasks() {
    return this.pcTasks;
  }
}
