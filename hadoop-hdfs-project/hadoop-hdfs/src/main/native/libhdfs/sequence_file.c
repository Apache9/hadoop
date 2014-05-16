/**
 * Copyright 2012, Xiaomi.com.
 * All rights reserved.
 * Author: yehangjun
 */

#include "sequence_file.h"

#include "exception.h"
#include "hdfs.h"
#include "jni_helper.h"


#define WRITER_CLASS "com/xiaomi/infra/hadoop/io/SequenceFileWriter"


SequenceFileWriter SequenceFileCreateWriter(
    const char* path, const char* compression_type, const char* codec_name) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return NULL;
  }

  jstring jPath = NULL, jCompressionType = NULL, jCodecName = NULL;
  jobject jWriter = NULL;
  jthrowable jThr;
  jobject jRet = NULL;
  int ret = -1;

  jPath = (*env)->NewStringUTF(env, path);
  if (!jPath) {
    ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
    goto done;
  }
  jCompressionType = (*env)->NewStringUTF(env, compression_type);
  if (!jCompressionType) {
    ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
    goto done;
  }
  jCodecName = (*env)->NewStringUTF(env, codec_name);
  if (!jCodecName) {
    ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL, "OOM");
    goto done;
  }

  // Create an object of com.xiaomi.infra.hadoop.io.SequenceFileWriter.
  jThr = constructNewObjectOfClass(env, &jWriter,
      WRITER_CLASS,  // Class name.
      "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",  // Signature.
      jPath, jCompressionType, jCodecName); // Constructor arguments.
  if (jThr) {
    ret = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileCreateWriter(%s): constructNewObjectOfClass", path);
    goto done;
  }

  jRet = (*env)->NewGlobalRef(env, jWriter);
  if (!jRet) {
    ret = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
        "SequenceFileCreateWriter(%s): NewGlobalRef", path);
    goto done;
  }
  ret = 0;

done:
  destroyLocalReference(env, jPath);
  destroyLocalReference(env, jCompressionType);
  destroyLocalReference(env, jCodecName); 
  destroyLocalReference(env, jWriter); 

  if (ret) {
    return NULL;
  }
  return (SequenceFileWriter) jRet;
}

int SequenceFileAppend(
    SequenceFileWriter writer, const char* value, int value_length) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return -1;
  }

  // Sanity check.
  if (!writer) {
    errno = EBADF;
    return -1;
  }
  if (!value || value_length < 0) {
    errno = EINVAL;
    return -1;
  }

  jobject jWriter = (jobject) writer;
  jbyteArray jbValueArray = NULL;
  jthrowable jThr;
  int ret = -1;

  // Write the requisite bytes into the file
  jbValueArray = (*env)->NewByteArray(env, value_length);
  if (!jbValueArray) {
    errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
        "SequenceFileAppend(length = %d): NewByteArray", value_length);
    goto done;
  }

  (*env)->SetByteArrayRegion(env, jbValueArray, 0, value_length,
      (const jbyte*) value);
  if ((*env)->ExceptionCheck(env)) {
    errno = printPendingExceptionAndFree(env, PRINT_EXC_ALL,
        "SequenceFileAppend(length = %d): SetByteArrayRegion", value_length);
    goto done;
  }

  jThr = invokeMethod(env, NULL, INSTANCE, jWriter,
      WRITER_CLASS, "append", "([B)V", jbValueArray);
  if (jThr) {
    errno = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileAppend(length = %d): append", value_length);
    goto done;
  }
  ret = 0;

done:
  destroyLocalReference(env, jbValueArray);
  return ret;
}

int SequenceFileSync(SequenceFileWriter writer) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return -1;
  }

  // Sanity check.
  if (!writer) {
    errno = EBADF;
    return -1;
  }

  jobject jWriter = (jobject) writer;
  jthrowable jThr;

  jThr = invokeMethod(env, NULL, INSTANCE, jWriter,
      WRITER_CLASS, "sync", "()V");
  if (jThr) {
    errno = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileSync: sync");
    return -1;
  }
  return 0;
}

int SequenceFileHFlush(SequenceFileWriter writer) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return -1;
  }

  // Sanity check.
  if (!writer) {
    errno = EBADF;
    return -1;
  }

  jobject jWriter = (jobject) writer;
  jthrowable jThr;

  jThr = invokeMethod(env, NULL, INSTANCE, jWriter,
      WRITER_CLASS, "hflush", "()V");
  if (jThr) {
    errno = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileHFlush: hflush");
    return -1;
  }
  return 0;
}

int SequenceFileHSync(SequenceFileWriter writer) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return -1;
  }

  // Sanity check.
  if (!writer) {
    errno = EBADF;
    return -1;
  }

  jobject jWriter = (jobject) writer;
  jthrowable jThr;

  jThr = invokeMethod(env, NULL, INSTANCE, jWriter,
      WRITER_CLASS, "hsync", "()V");
  if (jThr) {
    errno = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileHSync: hsync");
    return -1;
  }
  return 0;
}

int SequenceFileClose(SequenceFileWriter writer) {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    errno = EINTERNAL;
    return -1;
  }

  // Sanity check.
  if (!writer) {
    errno = EBADF;
    return -1;
  }

  jobject jWriter = (jobject) writer;
  jthrowable jThr;
  int ret;

  jThr = invokeMethod(env, NULL, INSTANCE, jWriter,
      WRITER_CLASS, "close", "()V");
  if (jThr) {
    errno = printExceptionAndFree(env, jThr, PRINT_EXC_ALL,
        "SequenceFileClose: close");
    ret = -1;
  } else {
    ret = 0;
  }

  (*env)->DeleteGlobalRef(env, jWriter);
  return ret;
}
