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

package io.siddhi.extension.io.azurestorage.source;

import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.azurestorage.util.Constant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class will read a specific file with the given options and executed the action after processing.
 */
public class FileReaderTask implements Runnable {
    private static final Log log = LogFactory.getLog(FileReaderTask.class);
    private final SourceEventListener sourceEventListener;
    private final DataLakeFileClient dataLakeFileClient;
    private long bytesToBeReadAtOnce;
    private final boolean tailingEnabled;
    private final String[] requiredProperties;
    private final String actionAfterProcess;
    private final String moveAfterProcess;
    protected Map<String, Object> properties = new HashMap<>();
    private volatile boolean paused;
    private final ReentrantLock lock;
    private final Condition condition;
    private volatile boolean active;
    private final String siddhiAppName;
    private DataLakeSource.AzureDataLakeSourceState state;
    private final String filePath;
    private final long fileUpdateCheckInterval;

    public FileReaderTask(SourceEventListener sourceEventListener, DataLakeFileClient dataLakeFileClient,
                          long bytesToBeReadAtOnce, boolean tailingEnabled, String actionAfterProcess,
                          String moveAfterProcess, String[] requiredProperties, String siddhiAppName,
                          DataLakeSource.AzureDataLakeSourceState state, long fileUpdateCheckInterval) {
        this.dataLakeFileClient = dataLakeFileClient;
        this.filePath = dataLakeFileClient.getFilePath();
        this.sourceEventListener = sourceEventListener;
        this.bytesToBeReadAtOnce = bytesToBeReadAtOnce;
        this.tailingEnabled = tailingEnabled;
        this.actionAfterProcess = actionAfterProcess;
        this.moveAfterProcess = moveAfterProcess;
        this.requiredProperties = requiredProperties.clone();
        this.properties.put(Constant.FILE_PATH, dataLakeFileClient.getFilePath());
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.state = state;
        this.siddhiAppName = siddhiAppName;
        this.active = true;
        this.fileUpdateCheckInterval = fileUpdateCheckInterval;
        if (log.isDebugEnabled()) {
            log.debug("Initialized processing file: " + dataLakeFileClient.getFilePath() + " in Siddhi app: " +
                    siddhiAppName + ". Currently, " + state.getSnapshotInfoHashMap().get(filePath).getReadBytes() +
                    " bytes have read.");
        }
    }

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Processing file: " + dataLakeFileClient.getFilePath() + " in Siddhi app: " + siddhiAppName);
        }
        try {
            long fileSize = dataLakeFileClient.getProperties().getFileSize();
            ByteArrayOutputStream outputStream;
            String line = null;
            long readBytes = state.getSnapshotInfoHashMap().get(filePath).getReadBytes();
            String stringContent = state.getSnapshotInfoHashMap().get(filePath).getCurrentText();
            while (active && (readBytes < fileSize || tailingEnabled)) {
                if (paused) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
                outputStream = new ByteArrayOutputStream();
                if (line != null) {
                    properties.put(Constant.END_OF_FILE, false);
                    sourceEventListener.onEvent(line, getRequiredPropertyValues(properties));
                    line = null;
                }
                try {
                    if (readBytes < fileSize) {
                        dataLakeFileClient.readWithResponse(outputStream,
                                new FileRange(readBytes, bytesToBeReadAtOnce), null, null, false, null, Context.NONE);
                        readBytes += outputStream.toByteArray().length;
                        state.getSnapshotInfoHashMap().get(filePath).setReadBytes(readBytes);
                        String text = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
                        stringContent = stringContent.concat(text);
                        while (stringContent.indexOf('\n') != -1) {
                            line = stringContent.substring(0, stringContent.indexOf('\n'));
                            //checking there are lines after the current line break
                            //new string content
                            stringContent = stringContent.substring(stringContent.indexOf('\n') + 1);
                            state.getSnapshotInfoHashMap().get(filePath).setCurrentText(stringContent);
                            if (!stringContent.isEmpty()) {
                                properties.put(Constant.END_OF_FILE, false);
                                sourceEventListener.onEvent(line, getRequiredPropertyValues(properties));
                            }
                        }
                        try {
                            outputStream.close();
                            outputStream.reset();
                        } catch (IOException e) {
                            log.error("Error when closing ByteArrayOutputStream when reading file: " +
                                    dataLakeFileClient.getFilePath() + " in Siddhi app: " + siddhiAppName);
                        }
                    }
                    if (tailingEnabled && (readBytes > fileSize)) {
                        fileSize = dataLakeFileClient.getProperties().getFileSize();
                    }
                    if (readBytes == fileSize) {
                        // ignoring "The range specified is invalid for the current size of the resource" error.
                        if (line != null) {
                            properties.put(Constant.END_OF_FILE, true);
                            sourceEventListener.onEvent(line, getRequiredPropertyValues(properties));
                            line = null;
                        }
                        if (tailingEnabled) {
                            try {
                                try {
                                    while (fileSize == dataLakeFileClient.getProperties().getFileSize()) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Waiting since tailing enabled for file: " +
                                                    dataLakeFileClient.getFilePath() + " in Siddhi app: " +
                                                    siddhiAppName);
                                        }
                                        Thread.sleep(fileUpdateCheckInterval);
                                    }
                                } catch (DataLakeStorageException e) {
                                    //ignore
                                }
                                fileSize = dataLakeFileClient.getProperties().getFileSize();
                            } catch (InterruptedException ex) {
                                log.error("Error occurred during checking for new updates for file: " +
                                        dataLakeFileClient.getFilePath() + " wait in SiddhiApp: " + siddhiAppName);
                                Thread.currentThread().interrupt();
                            }
                        } else {
                            if (actionAfterProcess.equalsIgnoreCase(Constant.DELETE)) {
                                dataLakeFileClient.delete();
                                log.info("File: " + dataLakeFileClient.getFilePath() + " is deleted after " +
                                        "processing in Siddhi app: " + siddhiAppName + ".");
                            } else if (actionAfterProcess.equalsIgnoreCase(Constant.MOVE)) {
                                dataLakeFileClient.rename(dataLakeFileClient.getFileSystemName(), moveAfterProcess);
                                log.info("File: " + dataLakeFileClient.getFilePath() + " is moved to " +
                                        moveAfterProcess + " after processing in Siddhi app: " + siddhiAppName + ".");
                            } else {
                                log.info("File: " + dataLakeFileClient.getFilePath() + " finished processing " +
                                        "without deleting or moving in Siddhi app: " + siddhiAppName + ".");
                            }
                        }
                    }
                } catch (BlobStorageException e) {
                    log.error("Error when when processing file: " + dataLakeFileClient.getFilePath() +
                            " in Siddhi app: " + siddhiAppName + ".", e);
                }
            }
            if (line != null) {
                properties.put(Constant.END_OF_FILE, true);
                sourceEventListener.onEvent(line, getRequiredPropertyValues(properties));
            }
        } catch (Throwable e) {
            log.error("Error occurred while processing the file: " + dataLakeFileClient.getFilePath() +
                    " in Siddhi app: " + siddhiAppName + ".", e);
        }

    }

    private Object[] getRequiredPropertyValues(Map<String, Object> properties) {
        Object[] transportPropertyObjects = new Object[requiredProperties.length];
        int i = 0;
        for (String propertyKey : requiredProperties) {
            Object value = properties.get(propertyKey);
            if (value != null) {
                transportPropertyObjects[i++] = properties.get(propertyKey);
            } else {
                log.error("Failed to find required transport property '" + propertyKey + "' for " +
                        filePath + " in Siddhi app: " + siddhiAppName + ".");
            }
        }
        return transportPropertyObjects;
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void shutdownProcessor() {
        active = false;
    }
}
