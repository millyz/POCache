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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "erasure_code.h"
#include "gf_util.h"
#include "jni_common.h"
#include "org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder.h"

typedef struct _RSEncoder {
  IsalEncoder encoder;
  unsigned char* inputs[MMAX];
  unsigned char* outputs[MMAX];
} RSEncoder;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_initImpl(
JNIEnv *env, jobject thiz, jint numDataUnits, jint numParityUnits) {
  RSEncoder* rsEncoder = (RSEncoder*)malloc(sizeof(RSEncoder));
  memset(rsEncoder, 0, sizeof(*rsEncoder));
  initEncoder(&rsEncoder->encoder, (int)numDataUnits, (int)numParityUnits);

  setCoder(env, thiz, &rsEncoder->encoder.coder);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_encodeImpl(
JNIEnv *env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
jint dataLen, jobjectArray outputs, jintArray outputOffsets) {
  RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
  if (!rsEncoder) {
    THROW(env, "java/io/IOException", "NativeRSRawEncoder closed");
    return;
  }

  int numDataUnits = rsEncoder->encoder.coder.numDataUnits;
  int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
  int chunkSize = (int)dataLen;

  getInputs(env, inputs, inputOffsets, rsEncoder->inputs, numDataUnits);
  getOutputs(env, outputs, outputOffsets, rsEncoder->outputs, numParityUnits);

  encode(&rsEncoder->encoder, rsEncoder->inputs, rsEncoder->outputs, chunkSize);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_encodeUpdateImpl(
JNIEnv *env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
jint dataLen, jobjectArray outputs, jintArray outputOffsets, jint vecIdx) {
  RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);

  int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
  int chunkSize = (int)dataLen;
  int vecI = (int)vecIdx;

  getInputs(env, inputs, inputOffsets, rsEncoder->inputs, 1);
  //POCache added on Mar. 17, 2018
  if ((*env)->ExceptionCheck(env)) {// check whether exceptions occur
    (*env)->ExceptionDescribe(env);// print out exception description
    (*env)->ExceptionClear(env);// clear the exception
    free(rsEncoder);
    printf("JNI_encodeUpdateImpl() - Exception in getInputs()!\n");
    return;
  }

  getOutputs(env, outputs, outputOffsets, rsEncoder->outputs, numParityUnits);
  if ((*env)->ExceptionCheck(env)) {// check whether exceptions occur
    (*env)->ExceptionDescribe(env);// print out exception description
    (*env)->ExceptionClear(env);// clear the exception
    free(rsEncoder);
    printf("JNI_encodeUpdateImpl() - Exception in getOutputs()!\n");
    return;
  }

  encode_update(&rsEncoder->encoder, rsEncoder->inputs[0], rsEncoder->outputs, chunkSize, vecI);
  if ((*env)->ExceptionCheck(env)) {// check whether exceptions occur
    (*env)->ExceptionDescribe(env);// print out exception description
    (*env)->ExceptionClear(env);// clear the exception
    free(rsEncoder);
    printf("JNI_encodeUpdateImpl() - Exception in encode_update()!\n");
    return;
  }
  /* printf("JNI_encodeUpdateImpl() - complete encode_update()! chunkSize=%d\n", */
  /*         (int)chunkSize); */
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_destroyImpl(
JNIEnv *env, jobject thiz) {
  RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
  if (rsEncoder) {
    free(rsEncoder);
    setCoder(env, thiz, NULL);
  }
}
