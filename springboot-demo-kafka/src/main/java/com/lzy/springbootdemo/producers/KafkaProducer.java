package com.lzy.springbootdemo.producers;

import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class KafkaProducer {
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    /**
     * kafka的生产者,不带回调函数
     * @param message
     * @param session
     * @return
     */
    @GetMapping("/kafka/normal/{message}")
    public String sendMessage1(@PathVariable("message") String message, HttpSession session){
        kafkaTemplate.send("topic-test-lzy",message);
        return "ok";
    }

    /**
     * 带有回调函数的kafka生产者
     * @param callbackMessage
     */
    @GetMapping("/kafka/callback1/{message}")
    public void sendMessage2(@PathVariable("message") String callbackMessage) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("my-topic", callbackMessage);
        future.thenAccept(result->{
            // 消息发送到的topic
            String topic = result.getRecordMetadata().topic();
            // 消息发送到的分区
            int partition = result.getRecordMetadata().partition();
            // 消息在分区内的offset
            long offset = result.getRecordMetadata().offset();
            System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
        });
        future.exceptionally(e->{
            e.printStackTrace();
            System.out.println("发送消息失败:" + e.getMessage());
            return null;
        });
    }

    // 第一种 配置事务
    @GetMapping("/kafka/transaction")
    public void sendMessage7(){
        //使用executeInTransaction 需要在yml中配置事务参数，配置成功才能使用executeInTransaction方法，且运行报错后回滚
        kafkaTemplate.executeInTransaction(operations->{
            operations.send("topic1","test executeInTransaction");
            throw new RuntimeException("fail");
        });

        //没有在yml配置事务，这里就会出现消息发送成功，异常也出现了。如果配置事务，则改用executeInTransaction 替代send方法
        kafkaTemplate.send("topic1","test executeInTransaction");
        throw new RuntimeException("fail");
    }

    //第二种 配置事务 （注解方式）
    // [1] 需要在yml 配置 transaction-id-prefix: kafka_tx.
    // [2] 在方法上添加@Transactional(rollbackFor = RuntimeException.class)  做为开启事务并回滚
    // [3] 在注解方式中 任然可以使用.send方法，不需要使用executeInTransaction方法
    @GetMapping("/send2/{input}")
    @Transactional(rollbackFor = RuntimeException.class)
    public String sendToKafka2(@PathVariable String input){
//        this.template.send(topic,input);
        //事务的支持

        kafkaTemplate.send("topic1",input);
        if("error".equals(input))
        {
            throw new RuntimeException("input is error");
        }
        kafkaTemplate.send("topic1",input+"anthor");

        return "send success!"+input;

    }


}
