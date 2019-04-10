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

/** POCache added
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Info on parity caching
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ParityCacheInfo implements Serializable {

  private static final long serialVersionUID = 0x0079fe4e;

  private int datNum; // current datNumPcache
  private int parNum; // current parNumPcache
  private int codeType; // Only RS code currently
  private boolean isDone; // Indicate whether the encoding is done
  private int pending; // Indicate how far from the first command to encode
  private int parNumNew; // New parNumPcache, no need to update parity when parNumPcacheNew = parNumPcache
  private List<String> slowDNsPossible; // Slow DNs based on the straggler estimation

  public ParityCacheInfo() {
    this.datNum = 2;
    this.parNum = 1;
    this.codeType = 0; // RS coder
    this.isDone = false;
    this.pending = 0;
    this.parNumNew = this.parNum;
    this.slowDNsPossible = new ArrayList<>();
  }

  public ParityCacheInfo(int codeK, int codeM) {
    this.datNum = codeK;
    this.parNum = codeM;
    this.codeType = 0;// RS coder
    this.isDone = false;
    this.pending = 0;
    this.parNumNew = this.parNum;
    this.slowDNsPossible = new ArrayList<>();
  }

  public ParityCacheInfo(int codeK, int codeM, int codeType, boolean isDone, int pending) {
    this.datNum = codeK;
    this.parNum = codeM;
    this.codeType = codeType;
    this.isDone = isDone;
    this.pending = pending;
    this.parNumNew = this.parNum;
    this.slowDNsPossible = new ArrayList<>();
  }

  public ParityCacheInfo(int codeK, int codeM, int codeType, boolean isDone, int pending,
                         List<String> slowDNsPossible) {
    this.datNum = codeK;
    this.parNum = codeM;
    this.codeType = codeType;
    this.isDone = isDone;
    this.pending = pending;
    this.parNumNew = this.parNum;
    this.slowDNsPossible = slowDNsPossible;
  }

  public ParityCacheInfo(int codeK, int codeM, int codeType, boolean isDone, int pending,
                         int parNumPcacheNew) {
    this.datNum = codeK;
    this.parNum = codeM;
    this.codeType = codeType;
    this.isDone = isDone;
    this.pending = pending;
    this.parNumNew = parNumPcacheNew;
    this.slowDNsPossible = new ArrayList<>();
  }

  public ParityCacheInfo(int codeK, int codeM, int codeType, boolean isDone, int pending,
      int parNumNew, List<String> slowDNsPossbile) {
    this.datNum = codeK;
    this.parNum = codeM;
    this.codeType = codeType;
    this.isDone = isDone;
    this.pending = pending;
    this.parNumNew = parNumNew;
    this.slowDNsPossible = slowDNsPossible;
  }

  public int getDatNumPcache() { return this.datNum; }
  public int getParNumPcache() { return this.parNum; }
  public int getCodeType() { return this.codeType; }
  public boolean getIsDone() { return this.isDone; }
  public int getPending() { return this.pending; }
  public int getParNumPcacheNew() { return this.parNumNew; }

  public List<String> getSlowDNsPossible() { return this.slowDNsPossible; }
  public void setDatNumPcache(int newDatNum) { this.datNum = newDatNum; }
  public void setParNumPcache(int newParNum) { this.parNum = newParNum; }
  public void setCodeType(int newCodeType) { this.codeType = newCodeType; }

  public void setIsDone(boolean isDone) {
    this.isDone = isDone;
    this.parNum = this.parNumNew;
  }

  public void setPending(int pending) { this.pending = pending; }
  public void setParNumPcacheNew(int parNumPcacheNew) { this.parNumNew = parNumPcacheNew; }
  public void setSlowDNsPossible(List<String> slowDNsPossible) { this.slowDNsPossible = slowDNsPossible; }
  public void incPending(int incNum) { this.pending += incNum; }
  public String toString() {
    return new StringBuilder().append("ParityCacheInfo(").append(datNum)
      .append(",").append(parNum).append(",").append(codeType)
      .append(")")
      .toString();
  }
}
