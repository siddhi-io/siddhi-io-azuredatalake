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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * Class implementing the Test cases for Kafka Sink.
 */
public class AzureDataLakeSinkTestCase {
    static final Logger LOG = Logger.getLogger(AzureDataLakeSinkTestCase.class);
    private volatile int count;
    private final String accountName = "abc";
    private final String accountKey = "xxx";


    @BeforeClass
    public static void init() throws Exception {
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            throw new RemoteException("Exception caught when starting server", e);
        }
    }

    @BeforeMethod
    public void init2() {
        count = 0;
    }

    private void testHelperToPublishEventsToDataLake(List<String> topics, String fileName) throws InterruptedException {
        if (fileName == null) {
            fileName = "events_published.txt";
        }
        LOG.info("Test Helper for publishing events");
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

    @Test
    public void testDefaultPublisherDataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        LOG.info("Test Helper for publishing events");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "file.path='parentDir/subDir/events_published.txt', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"single_topic1", 55.6f, 100L});
        fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
        fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='delete'," +
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
        assertEquals(count, 3);
        siddhiAppRuntime2.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testDefaultPublisherWithCreatingContainerDataLake() {
        Util.deleteCreatedParentDirectory("parentDir");
        LOG.info("Creating test for validating parameters when publishing events.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "add.to.existing.blob.container='false'," +
                        "recreate.blob.container='false'," +
                        "file.path='parentDir/subDir/events_published.txt', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testDefaultPublisherWithInvalidDataLakePath() {
        Util.deleteCreatedParentDirectory("parentDir");
        LOG.info("Creating test for validating parameters when publishing events.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "add.to.existing.blob.container='false'," +
                        "recreate.blob.container='false'," +
                        "file.path='/parentDir/subDir/events_published.txt', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDefaultPublisherWithCreatingContainer2DataLake() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        testHelperToPublishEventsToDataLake(null, null);
        LOG.info("Creating test for recreating the blob container.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "add.to.existing.blob.container='false'," +
                        "recreate.blob.container='true'," +
                        "file.path='parentDir/subDir/events_published.txt', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(15000, new AtomicBoolean(false), 15000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testDynamicURLForFileDataLake() throws InterruptedException {
        LOG.info("Creating test for dynamically create path for the publishing file and validate using source's " +
                "trp properties.");
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
        fooStream.send(new Object[]{"parentDir", "subDir2", "events", "single_topic4", 55.6f, 100L});
        fooStream.send(new Object[]{"parentDir", "subDir2", "events", "single_topic5", 56.6f, 200L});
        fooStream.send(new Object[]{"parentDir", "subDir2", "events", "single_topic6", 57.6f, 300L});
        fooStream.send(new Object[]{"parentDir2", "subDir", "events", "single_topic7", 55.6f, 100L});
        fooStream.send(new Object[]{"parentDir2", "subDir", "events", "single_topic8", 56.6f, 200L});
        fooStream.send(new Object[]{"parentDir2", "subDir", "events", "single_topic9", 57.6f, 300L});
        Thread.sleep(3000);
        siddhiAppRuntime.shutdown();
        String query = "@App:name('TestExecutionPlan') " +
                "define stream FooStream (symbol string, price float, volume long, fileName string, eof string); " +
                "@source(type='azuredatalake', " +
                "account.name='" + accountName + "', " +
                "account.key='" + accountKey + "', " +
                "blob.container='samplecontainer', " +
                "dir.uri='{{parentDir}}/{{subDir}}', " +
                "action.after.process='delete'," +
                "@map( type='csv', delimiter=',', " +
                "@attributes(symbol = '0', price = '1', volume = '2', fileName = 'trp:file.path', eof = 'trp:eof')))" +
                "Define stream BarStream (symbol string, price float, volume long, fileName string, eof string);" +
                "from BarStream select symbol, price, volume, fileName, eof insert into FooStream;";
        SiddhiAppRuntime siddhiAppRuntime2 =
                siddhiManager.createSiddhiAppRuntime
                        (query.replace("{{parentDir}}", "parentDir").replace("{{subDir}}", "subDir"));
        siddhiAppRuntime2.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    AssertJUnit.assertEquals("parentDir/subDir/events.txt", event.getData(3));
                }
            }
        });
        siddhiAppRuntime2.start();
        SiddhiAppRuntime siddhiAppRuntime3 =
                siddhiManager.createSiddhiAppRuntime
                        (query.replace("{{parentDir}}", "parentDir").replace("{{subDir}}", "subDir2"));
        siddhiAppRuntime3.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    AssertJUnit.assertEquals("parentDir/subDir2/events.txt", event.getData(3));
                }
            }
        });
        siddhiAppRuntime3.start();
        SiddhiAppRuntime siddhiAppRuntime4 =
                siddhiManager.createSiddhiAppRuntime
                        (query.replace("{{parentDir}}", "parentDir2").replace("{{subDir}}", "subDir"));
        siddhiAppRuntime4.addCallback("FooStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
                for (Event event : events) {
                    AssertJUnit.assertEquals("parentDir2/subDir/events.txt", event.getData(3));
                }
            }
        });
        siddhiAppRuntime4.start();
        SiddhiTestHelper.waitForEvents(10000, 9, count, 10000);
        assertEquals(count, 9);
        siddhiAppRuntime2.shutdown();
        siddhiAppRuntime3.shutdown();
        siddhiAppRuntime4.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
        Util.deleteCreatedParentDirectory("parentDir2");
    }

    @Test
    public void testDataLakeFileAppend() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        LOG.info("Creating test for publishing events and appending to existing file.");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@sink(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "file.path='parentDir/subDir/events_published.txt', " +
                        "append='true', " +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"single_topic1", 55.6f, 100L});
        fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
        fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        siddhiAppRuntime.start();
        fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        fooStream.send(new Object[]{"single_topic4", 55.6f, 100L});
        fooStream.send(new Object[]{"single_topic5", 75.6f, 102L});
        fooStream.send(new Object[]{"single_topic6", 57.6f, 103L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "append='true', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='delete'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(5000, 6, count, 5000);
        assertEquals(count, 6);
        siddhiAppRuntime2.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
    }

    @Test
    public void testDataLakeFileWithoutAppend() throws InterruptedException {
        Util.deleteCreatedParentDirectory("parentDir");
        LOG.info("Creating test for publishing events and deleting if file exists before adding events.");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='azuredatalake', " +
                "account.name='" + accountName + "', " +
                "account.key='" + accountKey + "', " +
                "blob.container='samplecontainer', " +
                "file.path='parentDir/subDir/events_published.txt', " +
                "append='false', " +
                "@map(type='csv'))" +
                "Define stream BarStream (symbol string, price float, volume long);" +
                "from FooStream select symbol, price, volume insert into BarStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"single_topic1", 55.6f, 100L});
        fooStream.send(new Object[]{"single_topic2", 75.6f, 102L});
        fooStream.send(new Object[]{"single_topic3", 57.6f, 103L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        fooStream.send(new Object[]{"single_topic4", 55.6f, 100L});
        fooStream.send(new Object[]{"single_topic5", 75.6f, 102L});
        fooStream.send(new Object[]{"single_topic6", 57.6f, 103L});
        Thread.sleep(5000);
        siddhiAppRuntime.shutdown();
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@source(type='azuredatalake', " +
                        "account.name='" + accountName + "', " +
                        "account.key='" + accountKey + "', " +
                        "blob.container='samplecontainer', " +
                        "dir.uri='parentDir/subDir', " +
                        "append='true', " +
                        "file.name='events_published.txt'," +
                        "action.after.process='delete'," +
                        "@map(type='csv'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from BarStream select symbol, price, volume insert into FooStream;");
        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count += events.length;
            }
        });
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(5000, 3, count, 5000);
        assertEquals(count, 3);
        siddhiAppRuntime2.shutdown();
        Util.deleteCreatedParentDirectory("parentDir");
    }
}
