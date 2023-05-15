package utils;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@SuppressWarnings({"unused"})
public class KafkaProducerUtil {

    private static final String PUSH_MSG_LOG = "准备发送消息为：{}";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaAdminClient kafkaAdminClient;

    /**
     * 如果没有topic，则创建一个
     * @param topicName :
     * @param partitionNum :
     * @param replicaNum :
     * @return org.apache.kafka.clients.admin.CreateTopicsResult
     */
    public Boolean createTopic(String topicName, int partitionNum, int replicaNum){
        KafkaFuture<Set<String>> topics = kafkaAdminClient.listTopics().names();
        try {
            if (topics.get().contains(topicName)) {
                return true;
            }
            NewTopic newTopic = new NewTopic(topicName, partitionNum, (short) replicaNum);
            kafkaAdminClient.createTopics(Collections.singleton(newTopic));
            return true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 传入topic名称，json格式字符串的消息，生产者进行发送
     * @param topicName : topic名称
     * @param jsonStr : 消息json字符串
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, String jsonStr) {
        createTopic(topicName, 3, 2);
        log.info(PUSH_MSG_LOG, jsonStr);
        //发送消息
        return sendAsync(topicName, jsonStr);
    }

    /**
     * 传入topic名称，json格式字符串数组的消息，生产者进行发送
     * @param topicName : topic名称
     * @param jsonStrs : 消息json字符串数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, String[] jsonStrs) {
        createTopic(topicName, 5, 5);
        int msgLength = jsonStrs.length;
        Boolean[] success = new Boolean[msgLength];
        for (int i = 0; i < msgLength; i++) {
            String jsonStr = jsonStrs[i];
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息
            success[i]=sendAsync(topicName,jsonStr);
        }
        return success;
    }

    /**
     * 传入topic名称，消息对象，生产者进行发送
     * @param topicName : topic名称
     * @param obj : 消息对象
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, Object obj) {
        createTopic(topicName, 5, 5);
        JSON objJson = JSONUtil.parse(obj);
        String jsonStr = objJson.toJSONString(0);
        log.info(PUSH_MSG_LOG, jsonStr);
        //发送消息
        return sendAsync(topicName, jsonStr);
    }

    /**
     * 传入topic名称，消息对象数组，生产者进行发送
     * @param topicName : topic名称
     * @param list : 消息对象数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, List<Object> list) {
        createTopic(topicName, 5, 5);
        Boolean[] success = new Boolean[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object obj = list.get(i);
            JSON objJson = JSONUtil.parse(obj);
            String jsonStr = objJson.toJSONString(0);
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息
            success[i] = sendAsync(topicName, jsonStr);
        }
        return success;
    }

    /**
     * 传入topic名称，json格式字符串的消息，生产者进行发送
     * @param topicName : topic名称
     * @param key : 消息key
     * @param jsonStr : 消息json字符串
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, String key, String jsonStr) {
        createTopic(topicName, 5, 5);
        log.info(PUSH_MSG_LOG, jsonStr);
        //发送消息
        return sendAsync(topicName,key, jsonStr);
    }

    /**
     * 传入topic名称，json格式字符串数组的消息，生产者进行发送
     * @param topicName : topic名称
     * @param key : 消息key
     * @param jsonStrs : 消息json字符串数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, String key, String[] jsonStrs) {
        createTopic(topicName, 5, 5);
        int msgLength = jsonStrs.length;
        Boolean[] success = new Boolean[msgLength];
        for (int i = 0; i < msgLength; i++) {
            String jsonStr = jsonStrs[i];
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息
            success[i] = sendAsync(topicName,key, jsonStr);
        }
        return success;
    }

    /**
     * 传入topic名称，消息对象，生产者进行发送
     * @param topicName : topic名称
     * @param key : 消息key
     * @param obj : 消息对象
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, String key, Object obj) {
        createTopic(topicName, 5, 5);
        JSON objJson = JSONUtil.parse(obj);
        String jsonStr = objJson.toJSONString(0);
        log.info(PUSH_MSG_LOG, jsonStr);


        return sendAsync(topicName,key, jsonStr);
    }

    /**
     * 传入topic名称，消息对象数组，生产者进行发送
     * @param topicName : topic名称
     * @param key : 消息key
     * @param list : 消息对象数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, String key, List<Object> list) {
        createTopic(topicName, 5, 5);
        Boolean[] success = new Boolean[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object obj = list.get(i);
            JSON objJson = JSONUtil.parse(obj);
            String jsonStr = objJson.toJSONString(0);
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息

            success[i] = sendAsync(topicName,key, jsonStr);
        }
        return success;
    }

    /**
     * 传入topic名称，json格式字符串的消息，生产者进行发送
     * @param topicName : topic名称
     * @param partition : 消息发送分区
     * @param key : 消息key
     * @param jsonStr : 消息json字符串
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, int partition, String key, String jsonStr) {
        createTopic(topicName, 5, 5);
        log.info(PUSH_MSG_LOG, jsonStr);
        //发送消息
        return sendAsync(topicName,partition,key, jsonStr);
    }

    /**
     * 传入topic名称，json格式字符串数组的消息，生产者进行发送
     * @param topicName : topic名称
     * @param partition : 消息发送分区
     * @param key : 消息key
     * @param jsonStrs : 消息json字符串数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, int partition, String key, String[] jsonStrs) {
        createTopic(topicName, 5, 5);
        int msgLength = jsonStrs.length;
        Boolean[] success = new Boolean[msgLength];
        for (int i = 0; i < msgLength; i++) {
            String jsonStr = jsonStrs[i];
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息
            success[i] = sendAsync(topicName,partition,key, jsonStr);
        }
        return success;
    }

    /**
     * 传入topic名称，消息对象，生产者进行发送
     * @param topicName : topic名称
     * @param partition : 消息发送分区
     * @param key : 消息key
     * @param obj : 消息对象
     * @return boolean : 推送是否成功
     */
    public boolean sendMessage(String topicName, int partition, String key, Object obj) {
        createTopic(topicName, 5, 5);
        JSON objJson = JSONUtil.parse(obj);
        String jsonStr = objJson.toJSONString(0);
        log.info(PUSH_MSG_LOG, jsonStr);
        //发送消息

        return sendAsync(topicName,partition,key, jsonStr);
    }

    /**
     * 传入topic名称，消息对象数组，生产者进行发送
     * @param topicName : topic名称
     * @param partition : 消息发送分区
     * @param key : 消息key
     * @param list : 消息对象数组
     * @return boolean : 推送是否成功
     */
    public Boolean[] sendMessage(String topicName, int partition, String key, List<Object> list) {
        createTopic(topicName, 5, 5);
        Boolean[] success = new Boolean[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object obj = list.get(i);
            JSON objJson = JSONUtil.parse(obj);
            String jsonStr = objJson.toJSONString(0);
            log.info(PUSH_MSG_LOG, jsonStr);
            //发送消息
            success[i] = sendAsync(topicName,partition,key, jsonStr);
        }
        return success;
    }
    public RecordMetadata sendSync(String topic, String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);
            SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
            return sendResult.getRecordMetadata();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送同步消息失败", e);
            throw new KafkaException("发送同步消息失败", e);
        }
    }
    public RecordMetadata sendSync(String topic, String key ,String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key,message);
            SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
            return sendResult.getRecordMetadata();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送同步消息失败", e);
            throw new KafkaException("发送同步消息失败", e);
        }
    }
    public RecordMetadata sendSync(String topic,int partition, String key,String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic,partition,key,message);
            SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
            return sendResult.getRecordMetadata();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送同步消息失败", e);
            throw new KafkaException("发送同步消息失败", e);
        }
    }

    public boolean sendAsync(String topic, String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);
            future.thenAcceptAsync(it -> log.info(">>>>> topic = {}", it.getRecordMetadata().topic()));
            return true;
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送异步消息失败", e);
            throw new KafkaException("发送异步消息失败", e);
        }
    }
    public boolean sendAsync(String topic, String key,String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic,key, message);
            future.thenAcceptAsync(it -> log.info(">>>>> topic = {}", it.getRecordMetadata().topic()));
            return true;
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送异步消息失败", e);
            throw new KafkaException("发送异步消息失败", e);
        }
    }
    public boolean sendAsync(String topic, int partition,String key,String message) {
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic,partition,key, message);
            future.thenAcceptAsync(it -> log.info(">>>>> topic = {}", it.getRecordMetadata().topic()));
            return true;
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("发送异步消息失败", e);
            throw new KafkaException("发送异步消息失败", e);
        }
    }
}