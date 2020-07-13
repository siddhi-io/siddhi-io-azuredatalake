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

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.azurestorage.util.Constant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is used to listen to a directory and create new File Reader tasks if new files are added.
 */
public class DirectoryReaderTask implements Runnable {
    private static final Log log = LogFactory.getLog(DirectoryReaderTask.class);
    private final SourceEventListener sourceEventListener;
    private final DataLakeFileSystemClient dataLakeFileSystemClient;
    private final String directoryPath;
    private Map<String, DataLakeSource.FileClientInfo> fileClientMap;
    private final SiddhiAppContext siddhiAppContext;
    private final long bytesToBeReadAtOnce;
    private final boolean isTailingEnabled;
    private final String[] requiredProperties;
    private final String actionAfterProcess;
    private final String moveAfterProcess;
    private volatile boolean paused;
    private final ReentrantLock lock;
    private final Condition condition;
    private volatile boolean active;
    private DataLakeSource.AzureDataLakeSourceState state;
    private long directoryCheckInterval;
    private long fileUpdateCheckInterval;

    public DirectoryReaderTask(DataLakeFileSystemClient dataLakeFileSystemClient, String directoryPath,
                               Map<String, DataLakeSource.FileClientInfo> fileClientMap,
                               SiddhiAppContext siddhiAppContext, SourceEventListener sourceEventListener,
                               long bytesToBeReadAtOnce, boolean isTailingEnabled, String actionAfterProcess,
                               String moveAfterProcess, String[] requiredProperties,
                               DataLakeSource.AzureDataLakeSourceState state, long directoryCheckInterval,
                               long fileUpdateCheckInterval) {
        this.sourceEventListener = sourceEventListener;
        this.dataLakeFileSystemClient = dataLakeFileSystemClient;
        this.directoryPath = directoryPath;
        this.fileClientMap = fileClientMap;
        this.siddhiAppContext = siddhiAppContext;
        this.bytesToBeReadAtOnce = bytesToBeReadAtOnce;
        this.isTailingEnabled = isTailingEnabled;
        this.actionAfterProcess = actionAfterProcess;
        this.moveAfterProcess = moveAfterProcess;
        this.requiredProperties = requiredProperties.clone();
        this.state = state;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.active = true;
        this.directoryCheckInterval = directoryCheckInterval;
        this.fileUpdateCheckInterval = fileUpdateCheckInterval;
    }

    @Override
    public void run() {
        while (active) {
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
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(directoryPath);
            PagedIterable<PathItem> directoryPathItemIterator = dataLakeFileSystemClient.listPaths(options, null);
            List<String> currentFiles = new ArrayList<>();
            for (PathItem pathItem : directoryPathItemIterator) {
                String filePath = pathItem.getName();
                if (!pathItem.isDirectory()) {
                    currentFiles.add(filePath);
                    if (fileClientMap.get(filePath) == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("New file detected: " + filePath + " for Siddhi App: " +
                                    siddhiAppContext.getName());
                        }
                        String[] directoryList = filePath.split(Constant.AZURE_FILE_SEPARATOR);
                        DataLakeDirectoryClient directoryClient =
                                dataLakeFileSystemClient.getDirectoryClient(directoryList[0]);
                        for (int i = 1; i < directoryList.length - 1; i++) {
                            directoryClient = directoryClient.getSubdirectoryClient(directoryList[i]);
                        }
                        fileClientMap.put(filePath,
                                new DataLakeSource.FileClientInfo(
                                        directoryClient.getFileClient(directoryList[directoryList.length - 1]), 0));
                        state.getSnapshotInfoHashMap().put(
                                filePath, new DataLakeSource.FileClientSnapshotInfo(filePath, 0, ""));
                        FileReaderTask fileReaderTask = new FileReaderTask(sourceEventListener,
                                fileClientMap.get(filePath).getDataLakeFileClient(),
                                bytesToBeReadAtOnce, isTailingEnabled, actionAfterProcess, moveAfterProcess,
                                requiredProperties, siddhiAppContext.getName(), state, fileUpdateCheckInterval);
                        fileClientMap.get((filePath)).setFileReaderTask(fileReaderTask);
                        fileClientMap.get((filePath)).setFileReaderTaskFuture(
                                siddhiAppContext.getExecutorService().submit(fileReaderTask));
                    }
                }
            }
            for (Map.Entry<String, DataLakeSource.FileClientInfo> entry : fileClientMap.entrySet()) {
                if (!currentFiles.contains(entry.getKey())) {
                    if (log.isDebugEnabled()) {
                        log.debug("Deleted file detected: " + entry.getKey() + " for Siddhi App: " +
                                siddhiAppContext.getName());
                    }
                    DataLakeSource.FileClientInfo fileClientInfo = fileClientMap.get(entry.getKey());
                    fileClientInfo.getFileReaderTaskFuture().cancel(true);
                    fileClientMap.remove(entry.getKey());
                }
            }
            try {
                Thread.sleep(directoryCheckInterval);
            } catch (InterruptedException ex) {
                log.error("Error occurred during DirectoryReaderTask wait in SiddhiApp: " +
                        siddhiAppContext.getName());
                Thread.currentThread().interrupt();
            }
        }
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
