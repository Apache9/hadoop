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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.util.SystemClock;

public class RMAppStateChangeLogger {
    static final Log LOG = LogFactory.getLog(RMAppStateChangeLogger.class);

    /**
     * This class is for logging the application state change.
     */
    static class ApplicationStateChange {
        // Escape sequences
        static final char EQUALS = '=';
        static final char[] charsToEscape =
                {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

        static class LogRecordBuilder {
            final StringBuilder buffer = new StringBuilder();

            // A little optimization for a very common case
            LogRecordBuilder add(String key, long value) {
                return _add(key, Long.toString(value));
            }

            <T> LogRecordBuilder add(String key, T value) {
                String escapedString = StringUtils.escapeString(String.valueOf(value),
                        StringUtils.ESCAPE_CHAR, charsToEscape).replaceAll("\n", "\\\\n")
                        .replaceAll("\r", "\\\\r");
                return _add(key, escapedString);
            }

            LogRecordBuilder _add(String key, String value) {
                if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
                buffer.append(key).append(EQUALS).append(value);
                return this;
            }

            @Override
            public String toString() {
                return buffer.toString();
            }
        }

        public static LogRecordBuilder createAppStateChangeLog(RMApp app, RMAppEvent event, RMAppState oldState,
                                                               RMAppState newState) {
            String trackingUrl = "N/A";
            RMAppAttempt attempt = app.getCurrentAppAttempt();
            long runTime;
            if (attempt != null) {
                trackingUrl = attempt.getTrackingUrl();
            }
            if (app.getFinishTime() <= app.getStartTime()) {
                runTime = new SystemClock().getTime() - app.getStartTime();
            } else {
                runTime = app.getFinishTime() - app.getStartTime();
            }
            LogRecordBuilder record = new LogRecordBuilder()
                    .add("appId", app.getApplicationId())
                    .add("event", event.getType())
                    .add("oldState", oldState)
                    .add("newState", newState)
                    .add("name", app.getName())
                    .add("user", app.getUser())
                    .add("queue", app.getQueue())
                    .add("state", app.getState())
                    .add("trackingUrl", trackingUrl)
                    .add("applicationType", app.getApplicationSubmissionContext().getApplicationType())
                    .add("memorySeconds", app.getRMAppMetrics().getMemorySeconds())
                    .add("vcoreSeconds", app.getRMAppMetrics().getVcoreSeconds())
                    .add("startTime", app.getStartTime())
                    .add("runTime", runTime);
            return record;
        }
    }

    public static void logAppStateChange(RMApp app, RMAppEvent event, RMAppState oldState, RMAppState newState) {
        if (app != null) {
            LOG.info(ApplicationStateChange.createAppStateChangeLog(app, event, oldState, newState));
        }
    }
}
