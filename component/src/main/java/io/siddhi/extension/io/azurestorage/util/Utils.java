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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Util Class.
 */
public class Utils {
    private static final Log log = LogFactory.getLog(Utils.class);
    /**
     * Returns the file path after validating and editing if there are additional slashes in front
     *
     * @param filePath directory path
     * @return filePath after validating
     */
    public static String validateAndGetFilePath(String filePath) {
        if (filePath.indexOf(Constant.AZURE_FILE_SEPARATOR) == 0) {
            filePath = filePath.replaceFirst(Constant.AZURE_FILE_SEPARATOR, "");
        }
        return filePath;
    }
    /**
     * Returns the file path after validating and editing if there are additional slashes in front
     *
     * @param directoryURI directory path
     * @return directoryURI after validating
     */
    public static String validateAndGetDirectoryPath(String directoryURI) {
        if (directoryURI.indexOf(Constant.AZURE_FILE_SEPARATOR) == 0) {
            directoryURI = directoryURI.replaceFirst(Constant.AZURE_FILE_SEPARATOR, "");
        }
        if (directoryURI.endsWith(Constant.AZURE_FILE_SEPARATOR)) {
            directoryURI = directoryURI.substring(0, directoryURI.length() - 1);
        }
        return directoryURI;
    }
}
