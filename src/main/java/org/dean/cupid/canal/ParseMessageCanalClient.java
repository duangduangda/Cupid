package org.dean.cupid.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dean.toolkit.AddressUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

/**
 * @description: 解析binlog(单机版)，并将变化的数据推至kafka
 * @author: dean
 * @create: 2019/07/08 14:50
 */
public class ParseMessageCanalClient {
    private static final Logger logger = LoggerFactory.getLogger(ParseMessageCanalClient.class);

    private static Producer<String, String> kafkaProducer = null;

    private static final String TOP_NAME = "canal";

    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(AddressUtils.getHostIp(), 11111),
                "example",
                "canal",
                "canal"
        );
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            // 统计120次空消息，如果连续出现120次空消息，结束客户端任务（测试的时候使用）
            int totalEmptyCount = 120;
            while (emptyCount < totalEmptyCount) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                try {
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        emptyCount++;
                        logger.info("Empty count:[{}]", emptyCount);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        emptyCount = 0;
                        printEntry(message.getEntries());
                    }
                    // 提交确认
                    connector.ack(batchId);
                } catch (Exception e) {
                    // 处理失败，回滚数据
                    connector.rollback(batchId);
                }
            }
            logger.info("Too many empty count.exit");
        } catch (Exception e) {
        } finally {
            connector.disconnect();
        }
    }

    /**
     * 输出消息体
     *
     * @param entries
     */
    private static void printEntry(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR### parse of eromanage-event has an error,data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 根据数据库不同的操作类型进行处理
                if (eventType == CanalEntry.EventType.DELETE) {
                    sendRowChange2Kafka(eventType, rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                    sendRowChange2Kafka(eventType, rowData.getAfterColumnsList());
                } else {
                    logger.info("Invalid event type.Ignore");
                }
            }
        }
    }

    /**
     * 将变化的数据推送至kafka
     *
     * @param eventType
     * @param columns
     */
    private static void sendRowChange2Kafka(CanalEntry.EventType eventType, List<CanalEntry.Column> columns) {
        JSONObject object = new JSONObject();
        object.put("event", eventType.name());
        for (CanalEntry.Column column : columns) {
            object.put(column.getName(), column.getValue());
        }
        // 消息推至kafka
        send2Kafka(object.toJSONString());
    }

    /**
     * 将变更的数据发送到kafka
     *
     * @param data
     */
    private static void send2Kafka(String data) {
        kafkaProducer = new KafkaProducer<String, String>(kafkaConfig());
        kafkaProducer.send(new ProducerRecord<String, String>(TOP_NAME, data));
    }

    /**
     * kafka配置
     *
     * @return
     */
    private static Properties kafkaConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("request.required.acks", 1);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);//32m
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}

