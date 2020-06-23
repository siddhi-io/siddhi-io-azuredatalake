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

package io.siddhi.extension.io.azurestorage;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.siddhi.extension.io.azurestorage.util.Constant;

import java.util.Locale;

public class Util {
    private static final String accountName = "wso2azuredatalakestore";
    private static final String accountKey =
            "6n/J8Z9aQ+1wWAV5USyipHsHfgcp2ZOeCb1FeHpyT/Lp/ET6S3kjQMUk2z24uIsBDxOMLjewuTCo18jO2MCMTw==";
    public static void deleteCreatedParentDirectory(String parentDirectory) {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, Constant.ENDPOINT, accountName);
        DataLakeServiceClient storageClient =
                new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
        DataLakeFileSystemClient dataLakeFileSystemClient = storageClient.getFileSystemClient("samplecontainer");
        DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient(parentDirectory);
        if (directoryClient.exists()) {
            directoryClient.deleteWithResponse(true, null, null, null);
        }
    }
}
