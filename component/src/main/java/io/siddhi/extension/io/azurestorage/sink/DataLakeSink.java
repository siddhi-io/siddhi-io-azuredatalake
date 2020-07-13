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

package io.siddhi.extension.io.azurestorage.sink;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
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
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.azurestorage.util.Constant;
import io.siddhi.extension.io.azurestorage.util.Utils;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Class representing Azure Data Lake Event Sink implementation.
 */
@Extension(
        name = "azuredatalake",
        namespace = "sink",
        description = "Azure Data Lake Sink can be used to publish (write) event data which is processed within " +
                "siddhi to file in Azure Data Lake.\nSiddhi-io-azuredatalake sink provides support to write both " +
                "textual and binary data into files.",
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
                        description = "The new or existing container name to be used.",
                        type = {DataType.STRING}),
                @Parameter(name = "add.to.existing.blob.container",
                        description = "This flag will be used to mention adding to an existing container with the " +
                                "same name is allowed.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(name = "recreate.blob.container",
                        description = "This flag will be used to recreate if there is an existing container with the " +
                                "same name.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "file.path",
                        description = "The absolute file path to the file in the blob container.",
                        type = {DataType.STRING}),
                @Parameter(name = "append",
                        description = "This flag is used to specify whether the data should be " +
                                "append to the file or not.\n" +
                                "If append = 'true', data will be write at the end of the file without " +
                                "changing the existing content.\n" +
                                "If append = 'false', if given file exists, existing content will be deleted and " +
                                "then data will be written back to the file.\n" +
                                "It is advisable to make append = 'true' when dynamic urls are used.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(name = "add.line.separator",
                        description = "This flag is used to specify whether events added to the file should " +
                                "be separated by a newline.\nIf add.event.separator= 'true', then a newline will be " +
                                "added after data is added to the file.\nIf the @map type is 'csv' the new line will " +
                                "be added by default",
                        optional = true,
                        type = {DataType.BOOL},
                        defaultValue = "true")
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
public class DataLakeSink extends Sink {
    private static final Log log = LogFactory.getLog(DataLakeSink.class);
    private DataLakeFileSystemClient dataLakeFileSystemClient;
    private String accountName;
    private Option filePathOption;
    private String blobContainer;
    boolean appendIfFileExists;
    private DataLakeFileClient fileClient;
    private DataLakeServiceClient storageClient;
    private boolean addLineSeparator;
    private long fileOffset;
    private SiddhiAppContext siddhiAppContext;
    private boolean containerExist;
    private Map<String, FileClientInfo> fileClientMap;
    private FileClientInfo currentFileClientInfo;
    private String currentFilePath;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, byte[].class, Object.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{Constant.FILE_PATH};
    }

    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                                SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        accountName = optionHolder.validateAndGetStaticValue(Constant.ACCOUNT_NAME);
        String accountKey = optionHolder.validateAndGetStaticValue(Constant.ACCOUNT_KEY);
        blobContainer = optionHolder.validateAndGetStaticValue(Constant.BLOB_CONTAINER);
        filePathOption = optionHolder.validateAndGetOption(Constant.FILE_PATH);
        appendIfFileExists =
                Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(Constant.APPEND_IF_FILE_EXISTS, "true"));
        boolean recreateBlobContainer = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(Constant.RECREATE_BLOB_CONTAINER, "false"));
        boolean addToExistingBlobContainer = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(Constant.ADD_TO_EXISTING_BLOB_CONTAINER, "true"));
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, Constant.ENDPOINT, accountName);
        storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        if (log.isDebugEnabled()) {
            log.debug("Successfully connected to Data Lake service and got the storage client for account: " +
                    accountName + " in Siddhi App: " + siddhiAppContext.getName());
            log.debug("Checking blob container " + blobContainer + " in the storage account in Siddhi App: " +
                    siddhiAppContext.getName());
        }
        containerExist = false;
        for (FileSystemItem fileSystemItem : storageClient.listFileSystems()) {
            if (log.isDebugEnabled()) {
                log.debug("Found File System with name: " + fileSystemItem.getName() + " in Siddhi App: " +
                        siddhiAppContext.getName());
            }
            if (fileSystemItem.getName().equalsIgnoreCase(blobContainer)) {
                containerExist = true;
                if (log.isDebugEnabled()) {
                    log.debug("Container exists for: " + fileSystemItem.getName());
                }
                if (addToExistingBlobContainer) {
                    if (log.isDebugEnabled()) {
                        log.debug("Existing container taken since add.to.existing.blob.container is 'true' in " +
                                "Siddhi App: " + siddhiAppContext.getName());
                    }
                    dataLakeFileSystemClient = storageClient.getFileSystemClient(blobContainer);
                } else {
                    if (recreateBlobContainer) {
                        if (log.isDebugEnabled()) {
                            log.debug("Existing container: " + blobContainer + " is deleted in Siddhi App: " +
                                    siddhiAppContext.getName());
                        }
                        dataLakeFileSystemClient = storageClient.getFileSystemClient(blobContainer);
                        dataLakeFileSystemClient.delete();
                        boolean containerCreated = false;
                        while (!containerCreated) {
                            try {
                                dataLakeFileSystemClient.create();
                                containerCreated = true;
                                if (log.isDebugEnabled()) {
                                    log.debug("Container " + blobContainer + " is recreated since " +
                                            "recreate.blob.container is 'true' in Siddhi App: " +
                                            siddhiAppContext.getName());
                                }
                            } catch (DataLakeStorageException e) {
                                //expected exception since it takes a while to delete the blob container
                                log.warn("Exception occurred when creating blob container: " + e.getMessage());
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException interruptedException) {
                                    log.warn("Exception occurred when waiting until the container is getting " +
                                            "created.");
                                }
                            }
                        }
                    } else {
                        throw new SiddhiAppCreationException("In 'Azure Data Lake' sink of the Siddhi app '" +
                                siddhiAppContext.getName() + "', " + Constant.RECREATE_BLOB_CONTAINER +
                                " parameter and " + Constant.ADD_TO_EXISTING_BLOB_CONTAINER +
                                " are set to 'false' and already a container with the name " + blobContainer +
                                " exists.");
                    }
                }
            }
        }
        String mapType = streamDefinition.getAnnotations().get(0).getAnnotations().get(0).getElements().get(0)
                .getValue();
        addLineSeparator = !mapType.equalsIgnoreCase("csv") &&
                (optionHolder.isOptionExists(Constant.ADD_LINE_SEPARATOR) ?
                        Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(Constant.ADD_LINE_SEPARATOR)) :
                        !mapType.equalsIgnoreCase("csv"));
        fileClientMap = new HashMap<>();
        currentFilePath = "";
        return null;
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        if (!containerExist) {
            if (log.isDebugEnabled()) {
                log.debug("Creating new container for name: " + blobContainer + " in storage account: " +
                        accountName + " in Siddhi App: " + siddhiAppContext.getName());
            }
            dataLakeFileSystemClient = storageClient.getFileSystemClient(blobContainer);
            dataLakeFileSystemClient.create();
        }
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        String filePath = filePathOption.getValue(dynamicOptions);
        if (currentFilePath.compareTo(filePath) != 0) {
            if (log.isDebugEnabled()) {
                log.debug("filePath: " + filePath + " is different from currentFilePath: " + currentFilePath);
            }
            currentFilePath = filePath;
            checkAndSetClient(filePath);
        }
        byte[] byteArray;
        if (payload instanceof byte[]) {
            byteArray = (byte[]) payload;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(payload.toString());
            if (this.addLineSeparator) {
                sb.append("\n");
            }
            byteArray = sb.toString().getBytes(StandardCharsets.UTF_8);
        }
        InputStream dataStream = new ByteArrayInputStream(byteArray);
        fileClient.append(dataStream, fileOffset, byteArray.length);
        fileClient.flush(fileOffset + byteArray.length);
        fileOffset += byteArray.length;
        currentFileClientInfo.setFileOffset(fileOffset);
        try {
            dataStream.close();
        } catch (IOException e) {
            log.error("Error while adding data to file from 'Azure Data Lake' sink of the Siddhi app '" +
                    siddhiAppContext.getName(), e);
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void destroy() {

    }

    private void checkAndSetClient(String filePath) {
        // check whether the parent directory exists
        filePath = Utils.validateAndGetFilePath(filePath);
        currentFileClientInfo = fileClientMap.get(filePath);
        if (currentFileClientInfo == null) {
            if (log.isDebugEnabled()) {
                log.debug("currentFileClientInfo not found in the fileClientMap");
            }
            PagedIterable<PathItem> storageIterable = dataLakeFileSystemClient.listPaths();
            java.util.Iterator<PathItem> parentDirectoryIterator = storageIterable.iterator();
            boolean parentDirExists = false;
            String[] directoryList = filePath.split(Constant.AZURE_FILE_SEPARATOR);
            String fileName = directoryList[directoryList.length - 1];
            while (parentDirectoryIterator.hasNext()) {
                if (parentDirectoryIterator.next().getName().equals(directoryList[0])) {
                    parentDirExists = true;
                }
            }
            DataLakeDirectoryClient directoryClient;
            if (!parentDirExists) {
                if (log.isDebugEnabled()) {
                    log.debug("Creating parent directory: " + directoryList[0] + " in " + blobContainer +
                            "for storage account: " + accountName + " in Siddhi App: " + siddhiAppContext.getName());
                }
                directoryClient = dataLakeFileSystemClient.createDirectory(directoryList[0]);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Parent directory exists: " + directoryList[0] + " in " + blobContainer +
                            "for storage account: " + accountName + " in Siddhi App: " + siddhiAppContext.getName());
                }
                directoryClient = dataLakeFileSystemClient.getDirectoryClient(directoryList[0]);
            }
            // checking the sub directories exists and creating if not.
            String directoryStructure = directoryList[0];
            int directories = 1;
            //directoryList.length - 1
            // since filePath contains the file name after the last Constant.AZURE_FILE_SEPARATOR
            while (directories < directoryList.length - 1) {
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
                    if (log.isDebugEnabled()) {
                        log.debug("Creating " + directoryList[directories] + ", since not found in: " +
                                directoryStructure + " in Siddhi App: " + siddhiAppContext.getName());
                    }
                    directoryClient = directoryClient.createSubdirectory(directoryList[directories]);
                } else {
                    directoryClient = directoryClient.getSubdirectoryClient(directoryList[directories]);
                }
                directoryStructure = directoryStructure.concat(Constant.AZURE_FILE_SEPARATOR +
                        directoryList[directories]);
                directories++;
            }
            ListPathsOptions options = new ListPathsOptions();
            options.setPath(directoryStructure);
            PagedIterable<PathItem> pagedIterable = dataLakeFileSystemClient.listPaths(options, null);
            java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
            if (log.isDebugEnabled()) {
                log.debug("Checking the " + fileName + " file in the path: " + directoryStructure +
                        " in Siddhi App: " + siddhiAppContext.getName());
            }
            boolean fileExists = false;
            while (iterator.hasNext()) {
                String name = iterator.next().getName();
                if (name.equals(filePath)) {
                    if (!appendIfFileExists) {
                        if (log.isDebugEnabled()) {
                            log.debug("Deleting and creating " + name + " file since : !appendIfFileExists in " +
                                    "Siddhi App: " + siddhiAppContext.getName());
                        }
                        directoryClient.deleteFile(fileName);
                        directoryClient.createFile(fileName);
                    }
                    fileExists = true;
                    break;
                }
            }
            if (!fileExists) {
                if (log.isDebugEnabled()) {
                    log.debug("Creating " + fileName + " at " + directoryStructure + " directory in Siddhi App: " +
                            siddhiAppContext.getName());
                }
                directoryClient.createFile(fileName);
            }
            fileClient = directoryClient.getFileClient(fileName);
            fileOffset = fileClient.getProperties().getFileSize();
            currentFileClientInfo = new FileClientInfo(fileClient, fileOffset);
            fileClientMap.put(filePath, currentFileClientInfo);
        } else {
            fileClient = currentFileClientInfo.getDataLakeFileClient();
            fileOffset = currentFileClientInfo.getFileOffset();
        }
    }

    private static class FileClientInfo {
        private final DataLakeFileClient dataLakeFileClient;
        private long fileOffset;

        public FileClientInfo(DataLakeFileClient dataLakeFileClient, long fileOffset) {
            this.dataLakeFileClient = dataLakeFileClient;
            this.fileOffset = fileOffset;
        }

        public DataLakeFileClient getDataLakeFileClient() {
            return dataLakeFileClient;
        }

        public long getFileOffset() {
            return fileOffset;
        }

        public void setFileOffset(long fileOffset) {
            this.fileOffset = fileOffset;
        }
    }
}
