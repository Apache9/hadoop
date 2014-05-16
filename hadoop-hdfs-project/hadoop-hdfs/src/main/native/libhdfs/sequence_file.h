/**
 * Copyright 2012, Xiaomi.com.
 * All rights reserved.
 * Author: yehangjun
 */

#ifndef LIBHDFS_SEQUENCE_FILE_H_
#define LIBHDFS_SEQUENCE_FILE_H_

#ifdef __cplusplus
extern  "C" {
#endif

/**
 * The C reflection of com.xiaomi.infra.hadoop.io.SequenceFileWriter
 */
struct SequenceFileWriterInternal;
typedef struct SequenceFileWriterInternal* SequenceFileWriter;

/** 
 * SequenceFileCreateWriter - Create a sequence file writer on the path, with
 * specified compression type and codec.
 * @param path The file path.
 * @param compression_type Could be "NONE", "RECORD", or "BLOCK".
 * @param codec_name Could be "default", "deflate", "snappy", "gzip", "bzip2",
 * or "lz4". "default" is the same codec as "deflate".
 * @return Returns a handle to the sequence file writer or NULL on error.
 */
SequenceFileWriter SequenceFileCreateWriter(
    const char* path, const char* compression_type, const char* codec_name);

/** 
 * SequenceFileAppend - Append a value to the sequence file, with a null key.
 * @param writer The handle of the sequence file writer.
 * @param value Buffer holding the value.
 * @param value_length The length of value in bytes. We don't assume the value
 * is null-terminated so the length must be passed in.
 * @return Returns 0 on success, -1 on error.  
 */
int SequenceFileAppend(
    SequenceFileWriter writer, const char* value, int value_length);

/** 
 * SequenceFileSync - Sync the sequence file by writing out a block.
 * See SequenceFile.Writer.sync() for the details.
 * @param writer The handle of the sequence file writer.
 * @return Returns 0 on success, -1 on error.  
 */
int SequenceFileSync(SequenceFileWriter writer);

/** 
 * SequenceFileHFlush - HFlush the underlying HDFS file.
 * See SequenceFile.Writer.hflush() for the details.
 * @param writer The handle of the sequence file writer.
 * @return Returns 0 on success, -1 on error.  
 */
int SequenceFileHFlush(SequenceFileWriter writer);

/** 
 * SequenceFileHSync - HSync the underlying HDFS file.
 * See SequenceFile.Writer.hsync() for the details.
 * @param writer The handle of the sequence file writer.
 * @return Returns 0 on success, -1 on error.  
 */
int SequenceFileHSync(SequenceFileWriter writer);

/** 
 * SequenceFileClose - Close the sequence file writer.
 * @param writer The handle of the sequence file writer.
 * @return Returns 0 on success, -1 on error.  
 */
int SequenceFileClose(SequenceFileWriter writer);

#ifdef __cplusplus
}
#endif

#endif // LIBHDFS_SEQUENCE_FILE_H_
