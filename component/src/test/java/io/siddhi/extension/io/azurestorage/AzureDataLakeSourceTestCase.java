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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * Class implementing the Test cases for Azure Data Lake Source.
 */
public class AzureDataLakeSourceTestCase {
    static final Logger LOG = Logger.getLogger(AzureDataLakeSourceTestCase.class);
    private int count;
    private final String accountName = "wso2azuredatalakestore";
    private final String accountKey =
            "6n/J8Z9aQ+1wWAV5USyipHsHfgcp2ZOeCb1FeHpyT/Lp/ET6S3kjQMUk2z24uIsBDxOMLjewuTCo18jO2MCMTw==";

    @BeforeMethod
    public void init2() {
        count = 0;
    }

    private void testHelperToPublishEventsToDataLake(List<String> topics, String fileName) throws InterruptedException {
        if (fileName == null) {
            fileName = "events_published.txt";
        }
        LOG.info("Test Helper for publishing events.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "file.path='parentDir/subDir/" + fileName + "', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        if (topics == null) {
            fooStream.send(new Object[]{"single_topic1", 55.6f, 100L});
            fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
            fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
        } else {
            for (int i = 0; i < topics.size(); i++) {
                fooStream.send(new Object[]{topics.get(i), 55.6f, 100L});
            }
        }
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
    }

    public void testHelperDynamicURLForFileDataLake() throws InterruptedException {
        LOG.info("Test Helper for publishing events while dynamically creating file path.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (parentDir string, subDir string, fileName string, symbol string, " +
                        "price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "file.path='{{parentDir}}/{{subDir}}/{{fileName}}.txt', " +
                        "@map(type='csv', @payload(symbol=\"0\",price=\"1\",volume=\"2\")))" +
                        "Define stream BarStream (parentDir string, subDir string, fileName string, symbol string, " +
                        "price float, volume long);" +
                        "from FooStream select parentDir, subDir, fileName, symbol, price, volume insert into " +
                        "BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic1", 55.6f, 100L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic2", 56.6f, 200L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic3", 57.6f, 300L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic4", 55.6f, 400L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic5", 56.6f, 500L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic6", 57.6f, 600L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic7", 55.6f, 700L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic8", 56.6f, 800L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic9", 57.6f, 900L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
    }

    public void testHelperForDynamicURLForFileDataLake2() throws InterruptedException {
        LOG.info("Test Helper 2 for publishing events while dynamically creating file path.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (parentDir string, subDir string, fileName string, symbol string, " +
                        "price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "file.path='{{parentDir}}/{{subDir}}/{{fileName}}.txt', " +
                        "@map(type='csv', @payload(symbol=\"0\",price=\"1\",volume=\"2\")))" +
                        "Define stream BarStream (parentDir string, subDir string, fileName string, symbol string, " +
                        "price float, volume long);" +
                        "from FooStream select parentDir, subDir, fileName, symbol, price, volume insert into " +
                        "BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic11", 55.6f, 100L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic22", 56.6f, 200L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events", "single_topic33", 57.6f, 300L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic44", 55.6f, 400L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic55", 56.6f, 500L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events2", "single_topic66", 57.6f, 600L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic77", 55.6f, 700L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic88", 56.6f, 800L});
        fooStream.send(new Object[]{"parentDir", "subDir", "events3", "single_topic99", 57.6f, 900L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void readingFileFromNonExistingBlobWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading from non existing blob container");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='nonExistingBlob', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readingFileFromNonExistingBlobWithAzureDataLake")
    public void readingFileFromValidatingPathWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading from non existing blob container");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='/parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readingFileFromValidatingPathWithAzureDataLake")
    public void readingFileFromValidatingPathWithAzureDataLake2() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading from non existing blob container");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='/parentDir/subDir/', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readingFileFromValidatingPathWithAzureDataLake2",
            expectedExceptions = SiddhiAppCreationException.class)
    public void readingFileFromNonExistingPathWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading from non existing file path");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir1/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readingFileFromNonExistingPathWithAzureDataLake",
            expectedExceptions = SiddhiAppCreationException.class)
    public void readingNotExistingFileWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading from non existing file path");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published1.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readingNotExistingFileWithAzureDataLake",
            expectedExceptions = SiddhiAppCreationException.class)
    public void validateParametersWithAzureDataLake1() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case 1 for validating parameters");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='delete'," +
                        "tailing.enabled='true'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "validateParametersWithAzureDataLake1",
            expectedExceptions = SiddhiAppCreationException.class)
    public void validateParametersWithAzureDataLake2() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case 2 for validating parameters");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='move'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "validateParametersWithAzureDataLake2")
    public void readingFileAndKeepUsingFileWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading a single file and keeping after it is read");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("single_topic1", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("single_topic2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("single_topic3", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 3);
    }

    @Test(dependsOnMethods = "readingFileAndKeepUsingFileWithAzureDataLake")
    public void readingFileAndMovingUsingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading a single file and moving after it is read");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='move'," +
                        "move.after.process='parentDir/movedDirectory/events_processed.txt'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 3);
        count = 0;
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/movedDirectory', " +
                        "file.name='events_processed.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("single_topic1", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("single_topic2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("single_topic3", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime2.shutdown();
        assertEquals(count, 3);
    }

    @Test(dependsOnMethods = "readingFileAndMovingUsingFileWithAzureDataLake")
    public void readingFileAndMoving2UsingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading a single file and moving to existing location after it is read");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/movedDirectory', " +
                        "file.name='events_processed.txt'," +
                        "action.after.process='move'," +
                        "move.after.process='parentDir/subDir/events_published.txt'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 3);
        count = 0;
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("single_topic1", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("single_topic2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("single_topic3", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime2.shutdown();
        assertEquals(count, 3);
    }

    @Test(dependsOnMethods = "readingFileAndMoving2UsingFileWithAzureDataLake")
    public void readingAndDeletingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading a single file and deleting by default after it is read");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("single_topic1", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("single_topic2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("single_topic3", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 3);
    }

    @Test(dependsOnMethods = "readingAndDeletingFileWithAzureDataLake",
            expectedExceptions = SiddhiAppCreationException.class)
    public void checkExceptionWhenTryingToNonExistingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading non existing file.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir/temp', " +
                        "file.name='events_published.txt'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 0, count, 5000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "checkExceptionWhenTryingToNonExistingFileWithAzureDataLake")
    public void readingFilesAndDeleteInDirectoryWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperDynamicURLForFileDataLake();
        LOG.info("Test case for reading a single file and tailing for content");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "action.after.process='delete'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        List<String> topics = new ArrayList<>();
        topics.add("single_topic1");
        topics.add("single_topic2");
        topics.add("single_topic3");
        topics.add("single_topic4");
        topics.add("single_topic5");
        topics.add("single_topic6");
        topics.add("single_topic7");
        topics.add("single_topic8");
        topics.add("single_topic9");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 9, count, 5000);
        siddhiAppRuntime.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
        assertEquals(count, 9);
    }

    @Test(dependsOnMethods = "readingFilesAndDeleteInDirectoryWithAzureDataLake")
    public void readingAndTailingFileWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        List<String> topics = new ArrayList<>();
        topics.add("single_topic1");
        topics.add("single_topic2");
        topics.add("single_topic3");
        testHelperToPublishEventsToDataLake(topics, null);
        LOG.info("Test case for reading a single file and tailing for content");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "tailing.enabled='true'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        List<String> topics1 = new ArrayList<>();
        topics1.add("single_topic1");
        topics1.add("single_topic2");
        topics1.add("single_topic3");
        topics1.add("single_topic4");
        topics1.add("single_topic5");
        topics1.add("single_topic6");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics1.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        assertEquals(count, 3);
        topics = new ArrayList<>();
        topics.add("single_topic4");
        topics.add("single_topic5");
        topics.add("single_topic6");
        testHelperToPublishEventsToDataLake(topics, null);
        SiddhiTestHelper.waitForEvents(7000, 6, count, 7000);
        assertEquals(count, 6);
        siddhiAppRuntime.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
    }

    @Test(dependsOnMethods = "readingAndTailingFileWithAzureDataLake")
    public void readingFilesAndTailingInDirectoryWithAzureDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperDynamicURLForFileDataLake();
        LOG.info("Test case for reading a single file and tailing for content");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "tailing.enabled='true'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        List<String> topics1 = new ArrayList<>();
        topics1.add("single_topic1");
        topics1.add("single_topic2");
        topics1.add("single_topic3");
        topics1.add("single_topic4");
        topics1.add("single_topic5");
        topics1.add("single_topic6");
        topics1.add("single_topic7");
        topics1.add("single_topic8");
        topics1.add("single_topic9");
        topics1.add("single_topic11");
        topics1.add("single_topic22");
        topics1.add("single_topic33");
        topics1.add("single_topic44");
        topics1.add("single_topic55");
        topics1.add("single_topic66");
        topics1.add("single_topic77");
        topics1.add("single_topic88");
        topics1.add("single_topic99");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics1.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 9, count, 5000);
        assertEquals(count, 9);
        testHelperForDynamicURLForFileDataLake2();
        SiddhiTestHelper.waitForEvents(10000, 18, count, 10000);
        assertEquals(count, 18);
        siddhiAppRuntime.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
    }

    //starting directory reading testcases
    @Test(dependsOnMethods = "readingFilesAndTailingInDirectoryWithAzureDataLake")
    public void readingFilesInDirectoryAndKeepUsingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading a files in a directory and keeping after it is read");
        Util.deleteCreatedParentDirectory("parentDir");
        List<String> topics = new ArrayList<>();
        topics.add("single_topic1");
        topics.add("single_topic2");
        topics.add("single_topic3");
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(topics, "events1.txt");
        topics = new ArrayList<>();
        topics.add("single_topic4");
        topics.add("single_topic5");
        topics.add("single_topic6");
        testHelperToPublishEventsToDataLake(topics, "events2.txt");
        List<String> topics1 = new ArrayList<>();
        topics1.add("single_topic1");
        topics1.add("single_topic2");
        topics1.add("single_topic3");
        topics1.add("single_topic4");
        topics1.add("single_topic5");
        topics1.add("single_topic6");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics1.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 6, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 6);
    }

    @Test(dependsOnMethods = "readingFilesInDirectoryAndKeepUsingFileWithAzureDataLake")
    public void readingFilesInDirectoryAndDeleteUsingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for reading a files in a directory and keeping after it is read");
        Util.deleteCreatedParentDirectory("parentDir");
        List<String> topics = new ArrayList<>();
        topics.add("single_topic1");
        topics.add("single_topic2");
        topics.add("single_topic3");
        testHelperToPublishEventsToDataLake(topics, "events1.txt");
        topics = new ArrayList<>();
        topics.add("single_topic4");
        topics.add("single_topic5");
        topics.add("single_topic6");
        testHelperToPublishEventsToDataLake(topics, "events2.txt");
        List<String> topics1 = new ArrayList<>();
        topics1.add("single_topic1");
        topics1.add("single_topic2");
        topics1.add("single_topic3");
        topics1.add("single_topic4");
        topics1.add("single_topic5");
        topics1.add("single_topic6");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "action.after.process='delete'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics1.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 6, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 6);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "action.after.process='delete'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        count = 0;
        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(5000, new AtomicBoolean(false), 5000);
        siddhiAppRuntime2.shutdown();
        assertEquals(count, 0);
    }

    @Test(dependsOnMethods = "readingFilesInDirectoryAndDeleteUsingFileWithAzureDataLake")
    public void readingFilesInDirectoryAndTailingUsingFileWithAzureDataLake() throws InterruptedException {
        LOG.info("Test case for tailing and listening files in a directory and keeping after it is read");
        Util.deleteCreatedParentDirectory("parentDir");
        List<String> topics = new ArrayList<>();
        topics.add("single_topic1");
        topics.add("single_topic2");
        topics.add("single_topic3");
        testHelperToPublishEventsToDataLake(topics, "events1.txt");
        topics = new ArrayList<>();
        topics.add("single_topic4");
        topics.add("single_topic5");
        topics.add("single_topic6");
        testHelperToPublishEventsToDataLake(topics, "events2.txt");
        List<String> topics1 = new ArrayList<>();
        topics1.add("single_topic1");
        topics1.add("single_topic2");
        topics1.add("single_topic3");
        topics1.add("single_topic4");
        topics1.add("single_topic5");
        topics1.add("single_topic6");
        topics1.add("tailing_topic1");
        topics1.add("tailing_topic2");
        topics1.add("tailing_topic3");
        topics1.add("tailing_topic4");
        topics1.add("tailing_topic5");
        topics1.add("tailing_topic6");
        topics1.add("new_file1");
        topics1.add("new_file2");
        topics1.add("new_file3");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "tailing.enabled='true'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    if (!topics1.contains(event.getData(0).toString())) {
                        AssertJUnit.fail("Received an unexpected event .");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 6, count, 5000);
        topics = new ArrayList<>();
        topics.add("tailing_topic1");
        topics.add("tailing_topic2");
        topics.add("tailing_topic3");
        testHelperToPublishEventsToDataLake(topics, "events1.txt");
        topics = new ArrayList<>();
        topics.add("tailing_topic4");
        topics.add("tailing_topic5");
        topics.add("tailing_topic6");
        testHelperToPublishEventsToDataLake(topics, "events2.txt");
        SiddhiTestHelper.waitForEvents(5000, 12, count, 5000);
        assertEquals(count, 12);
        topics = new ArrayList<>();
        topics.add("new_file1");
        topics.add("new_file2");
        topics.add("new_file3");
        testHelperToPublishEventsToDataLake(topics, "events3.txt");
        SiddhiTestHelper.waitForEvents(5000, new AtomicBoolean(false), 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 15);
    }

    //@Test(dependsOnMethods = "validateParametersWithAzureDataLake2")
    public void readingFileAndKeepUsingFileWithAzureDataLake2() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Test case for reading a single file and keeping after it is read");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='keep'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("single_topic1", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("single_topic2", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("single_topic3", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        siddhiAppRuntime.shutdown();
        assertEquals(count, 3);
    }
}
