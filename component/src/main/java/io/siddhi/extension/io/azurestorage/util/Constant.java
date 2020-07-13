/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.azurestorage.util;

/**
 * Constants used in siddhi-io-azuredatalake extension.
 */
public class Constant {
    public static final String ENDPOINT = "https://%s.dfs.core.windows.net";
    public static final String ACCOUNT_NAME = "account.name";
    public static final String ACCOUNT_KEY = "account.key";
    public static final String BLOB_CONTAINER = "blob.container";
    public static final String RECREATE_BLOB_CONTAINER = "recreate.blob.container";
    public static final String ADD_TO_EXISTING_BLOB_CONTAINER = "add.to.existing.blob.container";
    public static final String BYTES_TO_READ_FROM_FILE = "bytes.to.read.from.file";
    public static final String WAIT_TIME_TO_LOAD_CONTAINER = "waiting.time.to.load.container";
    public static final String TIME_INTERVAL_TO_CHECK_DIRECTORY = "time.interval.to.check.directory";
    public static final String TIME_INTERVAL_TO_CHECK_FOR_UPDATES = "time.interval.to.check.for.updates";
    public static final String FILE_PATH = "file.path";
    public static final String END_OF_FILE = "eof";
    public static final String FILE_NAME = "file.name";
    public static final String DIR_URI = "dir.uri";
    public static final String TAILING_ENABLED = "tailing.enabled";
    public static final String ACTION_AFTER_PROCESS = "action.after.process";
    public static final String MOVE_AFTER_PROCESS = "move.after.process";
    public static final String APPEND_IF_FILE_EXISTS = "append";
    public static final String ADD_LINE_SEPARATOR = "add.line.separator";
    public static final String FILE_OFFSET = "file.offset";
    public static final String AZURE_FILE_SEPARATOR = "/";
    public static final String MOVE = "move";
    public static final String DELETE = "delete";
    public static final String FILE_CLIENT_MAP = "file.client.map";
}
