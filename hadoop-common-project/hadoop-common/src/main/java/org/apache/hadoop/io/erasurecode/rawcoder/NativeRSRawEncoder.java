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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Reed-Solomon raw encoder using Intel ISA-L library.
 */
@InterfaceAudience.Private
public class NativeRSRawEncoder extends AbstractNativeRawEncoder {

  static {
    ErasureCodeNative.checkNativeCodeLoaded();
  }

  public NativeRSRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
    encoderLock.writeLock().lock();
    try {
      initImpl(coderOptions.getNumDataUnits(),
          coderOptions.getNumParityUnits());
    } finally {
      encoderLock.writeLock().unlock();
    }
  }

  @Override
  protected void performEncodeImpl(
          ByteBuffer[] inputs, int[] inputOffsets, int dataLen,
          ByteBuffer[] outputs, int[] outputOffsets) throws IOException {
    encodeImpl(inputs, inputOffsets, dataLen, outputs, outputOffsets);
  }

  //POCache added on May. 18, 2018
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs, int vecIdx) {
    int[] inputOffsets = new int[inputs.length];
    int[] outputOffsets = new int[outputs.length];
    int dataLen = inputs[0].remaining();
    for (int i = 0; i < inputs.length; i++) {
      inputOffsets[i] = inputs[i].position();
    }
    for (int i = 0; i < outputs.length; i++) {
      outputOffsets[i] = outputs[i].position();
    }
    encodeUpdateImpl(inputs, inputOffsets, dataLen, outputs, outputOffsets, vecIdx);
//    LOG.info("zmdebug NativeRSRawEncoder - after encodeUpdateImpl()!");
  }

  //POCache added on Mar. 5, 2018
  public void encode(byte[][] inputs, byte[][] outputs, int vecIdx) {
    ByteArrayEncodingState baeState= new ByteArrayEncodingState(
            this, inputs, outputs, vecIdx);
    if (baeState.encodeLength == 0) { return; }

    ByteBufferEncodingState bbeState = baeState.convertToByteBufferStateKeepOutputs();
    int[] inputOffsets = new int[bbeState.inputs.length];
    int[] outputOffsets = new int[bbeState.outputs.length];
    int dataLen = bbeState.inputs[0].remaining();

    ByteBuffer buffer;
    for (int i = 0; i < bbeState.inputs.length; i++) {
      buffer = bbeState.inputs[i];
      inputOffsets[i] = buffer.position();
    }

    for (int i = 0; i < bbeState.outputs.length; i++) {
      buffer = bbeState.outputs[i];
      outputOffsets[i] = buffer.position();
    }

    encodeUpdateImpl(bbeState.inputs, inputOffsets, dataLen,
            bbeState.outputs, outputOffsets, vecIdx);
//    LOG.info("zmdebug NativeRSRawEncoder - after encodeUpdateImpl()!");

    for (int i = 0; i < baeState.outputs.length; i++) {
      bbeState.outputs[i].get(baeState.outputs[i],
              baeState.outputOffsets[i], baeState.encodeLength);
    }
  }

  @Override
  public void release() {
    encoderLock.writeLock().lock();
    try {
      destroyImpl();
    } finally {
      encoderLock.writeLock().unlock();
    }
  }

  @Override
  public boolean preferDirectBuffer() {
    return true;
  }

  private native void initImpl(int numDataUnits, int numParityUnits);

  private native void encodeImpl(ByteBuffer[] inputs, int[] inputOffsets,
                                 int dataLen, ByteBuffer[] outputs,
                                 int[] outputOffsets) throws IOException;
  // ZM add on Mar. 5, 2018
  private native void encodeUpdateImpl(ByteBuffer[] inputs, int[] inputOffsets,
                                         int dataLen, ByteBuffer[] outputs,
                                         int[] outputOffsets, int vecIdx);

  private native void destroyImpl();
}
