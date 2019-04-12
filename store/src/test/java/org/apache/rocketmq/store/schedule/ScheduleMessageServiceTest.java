/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


public class ScheduleMessageServiceTest {

    String testMessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    /**
     * choose delay level
     */
    int delayLevel = 1;

    private static final String storePath = System.getProperty("user.home") + File.separator + "schedule_test#" + UUID.randomUUID();
    private static final int commitLogFileSize = 1024;
    private static final int cqFileSize = 10;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress bornHost;
    private static SocketAddress storeHost;
    DefaultMessageStore messageStore;
    MessageStoreConfig messageStoreConfig;
    BrokerConfig brokerConfig;
    ScheduleMessageService scheduleMessageService;

    static String sendMessage = " ------- schedule message test -------";
    static String topic = "schedule_topic_test";
    static String messageGroup = "delayGroupTest";


    static {
        try {
            bornHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            storeHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    @Before
    public void init() throws Exception {
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMessageDelayLevel(testMessageDelayLevel);
        messageStoreConfig.setMapedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMapedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        brokerConfig = new BrokerConfig();
        BrokerStatsManager manager = new BrokerStatsManager(brokerConfig.getBrokerClusterName());
        messageStore = new DefaultMessageStore(messageStoreConfig, manager, new MyMessageArrivingListener(), new BrokerConfig());

        assertThat(messageStore.load()).isTrue();

        messageStore.start();
        scheduleMessageService = messageStore.getScheduleMessageService();
    }


    @Test
    public void deliverDelayedMessageTimerTaskTest() throws Exception {
        MessageExtBrokerInner msg = buildMessage();

        int realQueueId = 0;

        // set delayLevel,and send delay message
        msg.setDelayTimeLevel(delayLevel);
        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.isOk()).isTrue();

        // make sure consumerQueue offset = commitLog offset
        StoreTestUtil.waitCommitLogReput(messageStore);

        // consumer message
        int delayQueueId = ScheduleMessageService.delayLevel2QueueId(delayLevel);
        assertThat(delayQueueId).isEqualTo(delayLevel - 1);

        Long offset = result.getAppendMessageResult().getLogicsOffset();

        // now, no message in queue,must wait > delayTime
        GetMessageResult messageResult = getMessage(realQueueId, offset);
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.NO_MESSAGE_IN_QUEUE);

        // timer run maybe delay, then consumer message again
        // and wait offsetTable
        TimeUnit.SECONDS.sleep(10);
        scheduleMessageService.buildRunningStats(new HashMap<String, String>());

        messageResult = getMessage(realQueueId, offset);
        // now,found the message
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.FOUND);

        // get the message body
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageResult.getBufferTotalSize());
        List<ByteBuffer> byteBufferList = messageResult.getMessageBufferList();
        for (ByteBuffer bb : byteBufferList) {
            byteBuffer.put(bb);
        }

        // warp and decode the message
        byteBuffer = ByteBuffer.wrap(byteBuffer.array());
        List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        String retryMsg = new String(msgList.get(0).getBody());
        assertThat(sendMessage).isEqualTo(retryMsg);

        //  method will wait 10s,so I run it by myself
        scheduleMessageService.persist();

        // add mapFile release
        messageResult.release();
    }

    /**
     * by jiangmin.wu wjm0729@aliyun.com
     */
    @Test
    public void randomDelayTaskTest() throws Exception {
        MessageExtBrokerInner msg = buildMessage();

        // 默认延迟等级
        // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h

        // 最好改为下面的形式
        // 1s 2s 3s 4s 5s 6s 7s 8s 9s 10s 20s 30s 40s 50s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 40m 50m 1h 2h 4h 8h 16h

        // 修改方法
        // broker.conf 配置文件增加
        // messageDelayLevel = 1s 2s 3s 4s 5s 6s 7s 8s 9s 10s 20s 30s 40s 50s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 40m 50m 1h 2h 4h 8h 16h

        // 7s is not defined
        long delayMillis = TimeUnit.SECONDS.toMillis(7);

        // real pub time
        msg.putUserProperty(MessageConst.PROPERTY_REAL_PUB_TIME, String.valueOf(System.currentTimeMillis() + delayMillis));
        msg.setDelayTimeLevel(scheduleMessageService.getMaxDelayLevelByMillis(delayMillis));

        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.isOk()).isTrue();

        int queueId = msg.getDelayTimeLevel() - 1;
        Long offset = result.getAppendMessageResult().getLogicsOffset();

        TimeUnit.SECONDS.sleep(4);

        // 4s after in 5s's queue
        GetMessageResult messageResult = messageStore.getMessage(messageGroup, ScheduleMessageService.SCHEDULE_TOPIC,
                queueId, offset, 1, null);
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.FOUND);

        // now real topic is empty
        long count = messageStore.getMessageTotalInQueue(topic, 0);
        assertThat(count).isEqualTo(0);

        TimeUnit.SECONDS.sleep(8);

        // 8s after msg will be find with real topic
        count = messageStore.getMessageTotalInQueue(topic, 0);
        assertThat(count).isEqualTo(1);

        // only one msg in queue
        // so queueId = 0 and offset = 0
        messageResult = getMessage(0, 0L);
        assertThat(messageResult.getStatus()).isEqualTo(GetMessageStatus.FOUND);

        // get the message body
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageResult.getBufferTotalSize());
        List<ByteBuffer> byteBufferList = messageResult.getMessageBufferList();
        for (ByteBuffer bb : byteBufferList) {
            byteBuffer.put(bb);
        }

        // warp and decode the message
        byteBuffer = ByteBuffer.wrap(byteBuffer.array());
        List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
        String retryMsg = new String(msgList.get(0).getBody());
        assertThat(sendMessage).isEqualTo(retryMsg);

        //  method will wait 10s,so I run it by myself
        scheduleMessageService.persist();

        // add mapFile release
        messageResult.release();
    }

    /**
     * add some [error/no use] code test
     */
    @Test
    public void otherTest() {
        // the method no use ,why need ?
        int queueId = ScheduleMessageService.queueId2DelayLevel(delayLevel);
        assertThat(queueId).isEqualTo(delayLevel + 1);

        // error delayLevelTest
        Long time = scheduleMessageService.computeDeliverTimestamp(999, 0);
        assertThat(time).isEqualTo(1000);

        // just decode
        scheduleMessageService.decode(new DelayOffsetSerializeWrapper().toJson());
    }


    private GetMessageResult getMessage(int queueId, Long offset) {
        return messageStore.getMessage(messageGroup, topic,
                queueId, offset, 1, null);
    }


    @After
    public void shutdown() throws InterruptedException {
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }


    public MessageExtBrokerInner buildMessage() {

        byte[] msgBody = sendMessage.getBytes();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("schedule_tag");
        msg.setKeys("schedule_key");
        msg.setBody(msgBody);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        return msg;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
