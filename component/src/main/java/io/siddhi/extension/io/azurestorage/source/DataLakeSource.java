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
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.azurestorage.util.Constant;
import io.siddhi.extension.io.azurestorage.util.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Class representing Azure Data Lake Event Source implementation.
 */
@Extension(
        name = "azuredatalake",
        namespace = "source",
        description = "Azure Data Lake Source can be used to read event data from files in Azure Data Lake and feed " +
                "data to Siddhi.",
        parameters = {
                @Parameter(name = "account.name",
                        description = "Azure storage account name to be used. This can be found in the storage " +
                                "account's settings under 'Access Keys'",
                        type = {DataType.STRING}),
                @Parameter(name = "account.key",
                        description = "Azure storage account key to be used. This can be found in the storage " +
                                "account's settings under 'Access Keys'",
                        type = {DataType.STRING}),
                @Parameter(name = "blob.container",
                        description = "The container name of the Azure Data Lake Storage.",
                        type = {DataType.STRING}),
                @Parameter(name = "dir.uri",
                        description = "The absolute file path to the file in the blob container to be read.",
                        type = {DataType.STRING}),
                @Parameter(name = "file.name",
                        description = "The absolute file path to the file in the blob container to be read.",
                        type = {DataType.STRING}),
                @Parameter(name = "bytes.to.read.from.file",
                        description = "The number of bytes to be read at once.",
                        optional = true,
                        type = {DataType.LONG},
                        defaultValue = "32768"),
                @Parameter(name = "tailing.enabled",
                        description = "The extension will continously check for the new content added to the files " +
                                "when this parameter is set to 'true'.",
                        optional = true,
                        type = {DataType.BOOL},
                        defaultValue = "false"),
                @Parameter(
                        name = "action.after.process",
                        description = "The action which should be carried out \n" +
                                "after processing a file in the given directory. \n" +
                                "It can be either `delete`, `move` or `keep` and default value will be 'DELETE'.\n" +
                                "If the action.after.process is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.process' parameter.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "delete"
                ),
                @Parameter(
                        name = "move.after.process",
                        description = "If action.after.process is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.process' parameter.\n" +
                                "This should be the absolute path of the file that going to be created after moving " +
                                "is done.\nThis has to be the relative path from the file system excluding the file " +
                                "system name.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "waiting.time.to.load.container",
                        description = "Extension will wait the mentioned time in this parameter until the blob " +
                                "container becomes available, before exiting with the SiddhiAppCreationException.\n",
                        type = {DataType.LONG},
                        optional = true,
                        defaultValue = "5000"
                ),
                @Parameter(
                        name = "time.interval.to.check.directory",
                        description = "Extension will check for new files in the given directory periodically with " +
                                "the given interval.\n",
                        type = {DataType.LONG},
                        optional = true,
                        defaultValue = "1000"
                ),
                @Parameter(
                        name = "time.interval.to.check.for.updates",
                        description = "Extension will continuously check for file updates in file/s periodically " +
                                "with the given interval when 'tailing.enabled' is set to 'true'.\n",
                        type = {DataType.LONG},
                        optional = true,
                        defaultValue = "1000"
                )
        },
        examples = {
                @Example(
                        syntax =
                                "@sink(\n" +
                                        "type='azuredatalake', \n" +
                                        "account.name='wso2datalakestore', \n" +
                                        "account.key=" +
                                        "'jUTCeBGgQgd7Wahm/tLGdFgoHuxmUC+KYzqiBKgKMt26gGp1Muk2U6gy34A3oqogQ4EX3+" +
                                        "9SGUlXKHQNALeYqQ==', \n" +
                                        "blob.container='samplecontainer', \n" +
                                        "file.path='parentDir/subDir/events.txt', \n" +
                                        "@map(type='csv')\n" +
                                        ")\n" +
                                        "Define stream BarStream (symbol string, price float, volume long);",

                        description = "" +
                                "Under above configuration, a file(events.txt) will be created(if not exists) in the " +
                                "storage account 'wso2datalakestore' in the parentDir/subDir/ path.\n" +
                                "For each event received to the sink, it will get appended to the file in csv format" +
                                "output will looks like below.\n" +
                                "WSO2,55.6,100\n" +
                                "IBM,55.7,102"
                )
        }
)
public class DataLakeSource extends Source<DataLakeSource.AzureDataLakeSourceState> {
    private static final Log log = LogFactory.getLog(DataLakeSource.class);
    private DataLakeFileSystemClient dataLakeFileSystemClient;
    String accountName;
    String accountKey;
    private String dirURI;
    private String directoryStructure;
    private String fileName;
    String blobContainer;
    DataLakeServiceClient storageClient;
    private boolean isTailingEnabled;
    private String actionAfterProcess;
    private String moveAfterProcess = null;
    private SourceEventListener sourceEventListener;
    private SiddhiAppContext siddhiAppContext;
    private long bytesToBeReadAtOnce;
    private Future<?> directoryReaderFuture;
    private String[] requiredProperties;
    private Map<String, DataLakeSource.FileClientInfo> fileClientMap;
    private DirectoryReaderTask directoryReaderTask;
    private long waitingTimeToLoadContainer;
    private long directoryCheckInterval;
    private long fileUpdateCheckInterval;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<AzureDataLakeSourceState> init(SourceEventListener sourceEventListener,
                                                       OptionHolder optionHolder, String[] requiredProperties,
                                                       ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.requiredProperties = requiredProperties.clone();
        this.sourceEventListener = sourceEventListener;
        this.siddhiAppContext = siddhiAppContext;
        fileClientMap = new ConcurrentHashMap<>();
        accountName = optionHolder.validateAndGetStaticValue(Constant.ACCOUNT_NAME);
        accountKey = optionHolder.validateAndGetStaticValue(Constant.ACCOUNT_KEY);
        blobContainer = optionHolder.validateAndGetStaticValue(Constant.BLOB_CONTAINER);
        dirURI = optionHolder.validateAndGetStaticValue(Constant.DIR_URI);
        fileName = optionHolder.validateAndGetStaticValue(Constant.FILE_NAME, null);
        isTailingEnabled = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(Constant.TAILING_ENABLED, "false"));
        bytesToBeReadAtOnce =
                Long.parseLong(optionHolder.validateAndGetStaticValue(Constant.BYTES_TO_READ_FROM_FILE, "32768"));
        waitingTimeToLoadContainer =
                Long.parseLong(optionHolder.validateAndGetStaticValue(Constant.WAIT_TIME_TO_LOAD_CONTAINER, "5000"));
        directoryCheckInterval =
                Long.parseLong(optionHolder.validateAndGetStaticValue(
                        Constant.TIME_INTERVAL_TO_CHECK_DIRECTORY, "1000"));
        fileUpdateCheckInterval =
                Long.parseLong(optionHolder.validateAndGetStaticValue(
                        Constant.TIME_INTERVAL_TO_CHECK_FOR_UPDATES, "1000"));
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, Constant.ENDPOINT, accountName);
        storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        log.info("Successfully connected to Data Lake service and got the storage client for account: " +
                accountName + " in Siddhi App: " + siddhiAppContext.getName());
        dirURI = Utils.validateAndGetDirectoryPath(dirURI);
        if (isTailingEnabled) {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constant.ACTION_AFTER_PROCESS, "keep");
        } else {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constant.ACTION_AFTER_PROCESS, "delete");
        }
        if (optionHolder.isOptionExists(Constant.MOVE_AFTER_PROCESS)) {
            moveAfterProcess = optionHolder.validateAndGetStaticValue(Constant.MOVE_AFTER_PROCESS);
        }
        validateDirURIInAzureDataLake();
        validateParameters();
        if (moveAfterProcess != null) {
            createDirectoryStructureIfNotExistForMoving(moveAfterProcess);
        }
        return AzureDataLakeSourceState::new;
    }

    private void validateParameters() {
        if (isTailingEnabled) {
            if (!actionAfterProcess.equalsIgnoreCase("keep")) {
                throw new SiddhiAppCreationException("Tailing has been enabled by user." +
                        "'action.after.process' should be 'keep' when tailing is enabled. " +
                        "Hence stopping the Siddhi app '" + siddhiAppContext.getName() + "'.");
            }
        }
        if (Constant.MOVE.equalsIgnoreCase(actionAfterProcess) && (moveAfterProcess == null)) {
            throw new SiddhiAppCreationException("'moveAfterProcess' has not been provided where it is mandatory when" +
                    " 'actionAfterProcess' is 'move'. Hence stopping the siddhi app '" +
                    siddhiAppContext.getName() + "'.");
        }
    }

    private void createDirectoryStructureIfNotExistForMoving(String filePath) {
        log.debug("create directory if not found for moving the file after processing.");
        PagedIterable<PathItem> storageIterable = dataLakeFileSystemClient.listPaths();
        java.util.Iterator<PathItem> parentDirectoryIterator = storageIterable.iterator();
        boolean parentDirExists = false;
        String[] directoryList = filePath.split(Constant.AZURE_FILE_SEPARATOR);
        while (parentDirectoryIterator.hasNext()) {
            if (parentDirectoryIterator.next().getName().equals(directoryList[0])) {
                parentDirExists = true;
            }
        }
        DataLakeDirectoryClient directoryClient;
        if (!parentDirExists) {
            log.debug("Creating parent directory: " + directoryList[0] + " in " + blobContainer +
                    "for storage account: " + accountName + " in Siddhi App: " + siddhiAppContext.getName() +
                    " for move.after.process");
            directoryClient = dataLakeFileSystemClient.createDirectory(directoryList[0]);
        } else {
            log.debug("Parent directory: " + directoryList[0] + " exists in blob container: " + blobContainer +
                    "for storage account: " + accountName + " in Siddhi App: " + siddhiAppContext.getName());
            directoryClient = dataLakeFileSystemClient.getDirectoryClient(directoryList[0]);
        }
        // checking the sub directories exists and creating if not.
        String directoryStructure = directoryList[0];
        int directories = 1;
        // directoryList.length - 1
        // since filePath contains the file name after the last Constant.AZURE_FILE_SEPARATOR
        while (directories < directoryList.length - 1) {
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(directoryStructure);
            PagedIterable<PathItem> pagedIterable = dataLakeFileSystemClient.listPaths(options, null);
            java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
            boolean directoryFound = false;
            while (iterator.hasNext()) {
                if (iterator.next().getName().
                        equals(directoryStructure.concat(Constant.AZURE_FILE_SEPARATOR + directoryList[directories]))) {
                    directoryFound = true;
                }
            }
            if (!directoryFound) {
                log.debug("Creating " + directoryList[directories] + ", since not found in: " +
                        directoryStructure + " in Siddhi App: " + siddhiAppContext.getName());
                directoryClient = directoryClient.createSubdirectory(directoryList[directories]);
            } else {
                directoryClient = directoryClient.getSubdirectoryClient(directoryList[directories]);
                log.debug(directoryList[directories] + " exists in: " +
                        directoryStructure + " in Siddhi App: " + siddhiAppContext.getName());
            }
            directoryStructure = directoryStructure.concat(Constant.AZURE_FILE_SEPARATOR + directoryList[directories]);
            directories++;
        }
    }

    private void validateDirURIInAzureDataLake() {
        long startTime = System.currentTimeMillis();
        for (FileSystemItem fileSystemItem : storageClient.listFileSystems()) {
            if (fileSystemItem.getName().equalsIgnoreCase(blobContainer)) {
                log.error("Found container: " + fileSystemItem.getName() + " in Siddhi App: " +
                        siddhiAppContext.getName());
                dataLakeFileSystemClient = storageClient.getFileSystemClient(blobContainer);
                break;
            }
            if (startTime + waitingTimeToLoadContainer < System.currentTimeMillis()) {
                break;
            }
        }
        if (dataLakeFileSystemClient == null) {
            throw new SiddhiAppCreationException("The blob container '" + blobContainer +
                    "' does not exist in the storage account '" + accountName +
                    "' specified in 'Azure Data Lake' source of the Siddhi app '" + siddhiAppContext.getName() + "'.");
        }
        DataLakeDirectoryClient directoryClient = null;
        PagedIterable<PathItem> storageIterable = dataLakeFileSystemClient.listPaths();
        java.util.Iterator<PathItem> parentDirectoryIterator = storageIterable.iterator();
        String[] directoryList = dirURI.split(Constant.AZURE_FILE_SEPARATOR);
        while (parentDirectoryIterator.hasNext()) {
            if (parentDirectoryIterator.next().getName().equals(directoryList[0])) {
                directoryClient = dataLakeFileSystemClient.getDirectoryClient(directoryList[0]);
            }
        }
        if (directoryClient == null) {
            throw new SiddhiAppCreationException("The parent directory '" + directoryList[0] + "' does not exist in " +
                    "blob container '" + blobContainer + "' in the storage account '" + accountName +
                    "' specified in 'Azure Data Lake' source of the Siddhi app '" + siddhiAppContext.getName() + "'.");
        }

        directoryStructure = directoryList[0];
        int directories = 1;
        while (directories < directoryList.length) {
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(directoryStructure);
            PagedIterable<PathItem> pagedIterable = dataLakeFileSystemClient.listPaths(options, null);
            java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
            boolean directoryFound = false;
            while (iterator.hasNext()) {
                if (iterator.next().getName().equals(directoryStructure.concat(Constant.AZURE_FILE_SEPARATOR +
                        directoryList[directories]))) {
                    directoryFound = true;
                }
            }
            if (!directoryFound) {
                throw new SiddhiAppCreationException("The sub directory '" + directoryList[directories] +
                        "' does not exist in " + directoryStructure + "  in blob container '" + blobContainer +
                        "' in the storage account '" + accountName + "' specified in 'Azure Data Lake' source of " +
                        "the Siddhi app '" + siddhiAppContext.getName() + "'.");
            } else {
                directoryClient = directoryClient.getSubdirectoryClient(directoryList[directories]);
            }
            directoryStructure = directoryStructure.concat(Constant.AZURE_FILE_SEPARATOR + directoryList[directories]);
            directories++;
        }

        ListPathsOptions options = new ListPathsOptions();
        options.setPath(directoryStructure);
        PagedIterable<PathItem> directoryPathItemIterator = dataLakeFileSystemClient.listPaths(options, null);
        java.util.Iterator<PathItem> iterator = directoryPathItemIterator.iterator();
        boolean fileExists = false;
        String filePath;
        if (fileName != null) {
            log.debug("Checking the file: " + fileName + " in the path: " + directoryStructure +
                    " in Siddhi App: " + siddhiAppContext.getName());
            filePath = directoryStructure + Constant.AZURE_FILE_SEPARATOR + fileName;
            while (iterator.hasNext()) {
                String name = iterator.next().getName();
                if (name.equals(filePath)) {
                    fileClientMap.put(filePath,
                            new FileClientInfo(directoryClient.getFileClient(fileName), 0));
                    fileExists = true;
                    break;
                }
            }
            if (!fileExists) {
                throw new SiddhiAppCreationException("The file: '" + filePath + " does not exist in blob container '" +
                        blobContainer + "' in the storage account '" + accountName +
                        "' specified in 'Azure Data Lake' source of the Siddhi app '" +
                        siddhiAppContext.getName() + "'.");
            }
        } else {
            log.debug("Checking files in the path: " + directoryStructure + " in Siddhi App: " +
                    siddhiAppContext.getName());
            while (iterator.hasNext()) {
                PathItem pathItem = iterator.next();
                if (!pathItem.isDirectory()) {
                    log.debug("Listing item: " + pathItem.getName() + " in Siddhi App: " +
                            siddhiAppContext.getName());
                    String[] filePathContent = pathItem.getName().split(Constant.AZURE_FILE_SEPARATOR);
                    fileName = filePathContent[filePathContent.length - 1];
                    fileClientMap.put(pathItem.getName(),
                            new FileClientInfo(directoryClient.getFileClient(fileName), 0));
                }
            }
        }
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, AzureDataLakeSourceState state)
            throws ConnectionUnavailableException {
        for (Map.Entry<String, FileClientInfo> fileClientEntry : fileClientMap.entrySet()) {
            FileClientSnapshotInfo fileClientInfoSnapshot =
                    state.getSnapshotInfoHashMap().get(fileClientEntry.getKey());
            if (fileClientInfoSnapshot == null) {
                state.getSnapshotInfoHashMap().put(
                        fileClientEntry.getKey(), new FileClientSnapshotInfo(fileClientEntry.getKey(), 0, ""));
            }
            FileReaderTask fileReaderTask = new FileReaderTask(sourceEventListener,
                    fileClientEntry.getValue().getDataLakeFileClient(), bytesToBeReadAtOnce, isTailingEnabled,
                    actionAfterProcess, moveAfterProcess, requiredProperties, siddhiAppContext.getName(), state,
                    fileUpdateCheckInterval);
            fileClientEntry.getValue().setFileReaderTask(fileReaderTask);
            fileClientEntry.getValue().
                    setFileReaderTaskFuture(siddhiAppContext.getExecutorService().submit(fileReaderTask));
        }
        for (Map.Entry<String, FileClientSnapshotInfo> entry : state.getSnapshotInfoHashMap().entrySet()) {
            if (fileClientMap.get(entry.getKey()) == null) {
                state.getSnapshotInfoHashMap().remove(entry.getKey());
                log.warn("The snapshotted file: " + entry.getKey() + " is not available for processing." +
                        " in Siddhi app: " + siddhiAppContext.getName() + ", hence removed from snapshot.");
            }
        }
        if (fileClientMap.size() > 1) {
            directoryReaderTask = new DirectoryReaderTask(dataLakeFileSystemClient, directoryStructure, fileClientMap,
                    siddhiAppContext, sourceEventListener, bytesToBeReadAtOnce, isTailingEnabled,
                    actionAfterProcess, moveAfterProcess, requiredProperties, state, directoryCheckInterval,
                    fileUpdateCheckInterval);
            directoryReaderFuture = siddhiAppContext.getExecutorService().submit(directoryReaderTask);
        }
    }

    @Override
    public void disconnect() {
        if (directoryReaderTask != null && !directoryReaderFuture.isCancelled()) {
            directoryReaderTask.shutdownProcessor();
            directoryReaderFuture.cancel(true);
            log.info("Disconnecting directory listener for Siddhi App: " + siddhiAppContext.getName());
        }
        for (Map.Entry<String, FileClientInfo> entry : fileClientMap.entrySet()) {
            FileClientInfo fileClientInfo = entry.getValue();
            if (!fileClientInfo.getFileReaderTaskFuture().isCancelled()) {
                fileClientInfo.getFileReaderTask().shutdownProcessor();
                fileClientInfo.getFileReaderTaskFuture().cancel(true);
                log.info("Disconnecting processing for file: " + entry.getKey() + " in Siddhi app: " +
                        siddhiAppContext.getName());
            }
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public void pause() {
        for (Map.Entry<String, FileClientInfo> entry : fileClientMap.entrySet()) {
            FileClientInfo fileClientInfo = entry.getValue();
            if (fileClientInfo.getFileReaderTask() != null) {
                fileClientInfo.getFileReaderTask().pause();
            }
        }
        if (directoryReaderFuture != null && !directoryReaderFuture.isDone()) {
            directoryReaderTask.pause();
        }
    }

    @Override
    public void resume() {
        if (fileClientMap.size() > 0) {
            for (Map.Entry<String, FileClientInfo> entry : fileClientMap.entrySet()) {
                FileClientInfo fileClientInfo = entry.getValue();
                if (fileClientInfo.getFileReaderTask() != null) {
                    fileClientInfo.getFileReaderTask().resume();
                }
            }
        }
        if (directoryReaderFuture != null && !directoryReaderFuture.isDone()) {
            directoryReaderTask.resume();
        }
    }

    /**
     * Class to hold the file's tasks and its client.
     */
    public static class FileClientInfo {
        private final DataLakeFileClient dataLakeFileClient;
        private FileReaderTask fileReaderTask;
        private Future<?> fileReaderTaskFuture;

        public FileClientInfo(DataLakeFileClient dataLakeFileClient, long readBytes) {
            this.dataLakeFileClient = dataLakeFileClient;
        }

        public DataLakeFileClient getDataLakeFileClient() {
            return dataLakeFileClient;
        }

        public FileReaderTask getFileReaderTask() {
            return fileReaderTask;
        }

        public void setFileReaderTask(FileReaderTask fileReaderTask) {
            this.fileReaderTask = fileReaderTask;
        }

        public Future<?> getFileReaderTaskFuture() {
            return fileReaderTaskFuture;
        }

        public void setFileReaderTaskFuture(Future<?> fileReaderTaskFuture) {
            this.fileReaderTaskFuture = fileReaderTaskFuture;
        }
    }


    /**
     * Class to hold the file's read bytes when initializing the file processor.
     */
    public static class FileClientSnapshotInfo implements java.io.Serializable {
        protected String filePath;
        protected long readBytes;
        protected String currentText = "";
        private static final long serialVersionUID = 1L;

        public FileClientSnapshotInfo (String filePath, long readBytes, String currentText) {
            this.filePath = filePath;
            this.readBytes = readBytes;
            this.currentText = currentText;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public long getReadBytes() {
            return readBytes;
        }

        public void setReadBytes(long readBytes) {
            this.readBytes = readBytes;
        }

        public String getCurrentText() {
            return currentText;
        }

        public void setCurrentText(String currentText) {
            this.currentText = currentText;
        }
    }

    /**
     * State class for Azure Data Lake source.
     */
    public static class AzureDataLakeSourceState extends State {
        private Map<String, DataLakeSource.FileClientSnapshotInfo> snapshotInfoHashMap = new HashMap<>();

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> currentState = new HashMap<>();
            for (Map.Entry<String, FileClientSnapshotInfo> entry : snapshotInfoHashMap.entrySet()) {
                FileClientSnapshotInfo fileClientInfo = entry.getValue();
                log.debug("Snapshotting read bytes for file: " +
                        fileClientInfo.getFilePath() + " is: " + fileClientInfo.getReadBytes());

            }
            currentState.put(Constant.FILE_CLIENT_MAP, snapshotInfoHashMap);
            return currentState;
        }

        @Override
        public void restore(Map<String, Object> state) {
            snapshotInfoHashMap =
                    (Map<String, DataLakeSource.FileClientSnapshotInfo>) state.get(Constant.FILE_CLIENT_MAP);
            for (Map.Entry<String, FileClientSnapshotInfo> entry : snapshotInfoHashMap.entrySet()) {
                FileClientSnapshotInfo fileClientInfo = entry.getValue();
                log.debug("Restore read bytes for file: " +
                        fileClientInfo.getFilePath() + " is: " + fileClientInfo.getReadBytes());

            }
        }

        public Map<String, FileClientSnapshotInfo> getSnapshotInfoHashMap() {
            return snapshotInfoHashMap;
        }

        public void setSnapshotInfoHashMap(Map<String, FileClientSnapshotInfo> snapshotInfoHashMap) {
            this.snapshotInfoHashMap = snapshotInfoHashMap;
        }
    }
}
