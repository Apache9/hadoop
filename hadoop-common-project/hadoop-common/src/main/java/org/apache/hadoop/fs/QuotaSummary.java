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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** Store the quota summary of a directory with quota. */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QuotaSummary implements Writable {
  private long nameCount;
  private long quota;
  private long spaceConsumed;
  private long spaceQuota;

  /** Constructor */
  public QuotaSummary(long nameCount, long quota, long spaceConsumed, long spaceQuota) {
    this.nameCount = nameCount;
    this.quota = quota;
    this.spaceConsumed = spaceConsumed;
    this.spaceQuota = spaceQuota;
  }

  /** @return the name count */
  public long getNameCount() {
    return nameCount;
  }

  /** Return the directory quota */
  public long getQuota() {
    return quota;
  }

  /** Retuns (disk) space consumed */
  public long getSpaceConsumed() {
    return spaceConsumed;
  }

  /** Returns (disk) space quota */
  public long getSpaceQuota() {
    return spaceQuota;
  }

  /** {@inheritDoc} */
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(nameCount);
    out.writeLong(quota);
    out.writeLong(spaceConsumed);
    out.writeLong(spaceQuota);
  }

  /** {@inheritDoc} */
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    this.nameCount = in.readLong();
    this.quota = in.readLong();
    this.spaceConsumed = in.readLong();
    this.spaceQuota = in.readLong();
  }

  /**
   * Output format: <----12----> <----12----> <----12----> <----15----> <----15----> <----15---->
   * QUOTA USED_QUATA REMAINNG_QUOTA SPACE_QUOTA USED_SPACE_QUOTA REMAINING_SPACE_QUOTA
   */
  private static final String STRING_FORMAT = "%12s %12s %12s %15s %15s %15s ";

  /** The header string */
  private static final String HEADER = String.format(STRING_FORMAT, "quota", "used quota",
    "remaining quota", "space quota", "used space", "remaining space");

  /**
   * Return the header of the output.
   * @return the header of the output
   */
  public static String getHeader() {
    return HEADER;
  }

  /**
   * Return the string representation of the object in the output format. Output name quota, name
   * count, remaining name quota, space quota, consumed space, remaining space quota
   * @return the string representation of the object
   */
  public String toString() {
    String quotaStr = "none";
    String quotaUse = "n/a";
    String quotaRem = "inf";
    String spaceQuotaStr = "none";
    String spaceQuotaUse = "n/a";
    String spaceQuotaRem = "inf";

    if (quota > 0) {
      quotaStr = Long.toString(quota);
      quotaUse = Long.toString(nameCount);
      quotaRem = Long.toString(quota - nameCount);
    }
    if (spaceQuota > 0) {
      spaceQuotaStr = Long.toString(spaceQuota);
      spaceQuotaUse = Long.toString(spaceConsumed);
      spaceQuotaRem = Long.toString(spaceQuota - spaceConsumed);
    }

    return String.format(STRING_FORMAT, quotaStr, quotaUse, quotaRem, spaceQuotaStr, spaceQuotaUse,
      spaceQuotaRem);
  }
}
