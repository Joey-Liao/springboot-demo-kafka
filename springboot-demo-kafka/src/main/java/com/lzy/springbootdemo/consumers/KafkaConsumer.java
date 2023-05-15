package com.lzy.springbootdemo.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class KafkaConsumer {

    @Autowired
    ConsumerFactory consumerFactory;

    //消息过滤器
    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory(){
        ConcurrentKafkaListenerContainerFactory factory=new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        //被过滤器的消息将被丢弃
        factory.setAckDiscarded(true);
        //消息过滤策略
        factory.setRecordFilterStrategy(consumerRecord -> {
            if(Integer.parseInt(consumerRecord.value().toString())%2==0){
                return false;
            }
            //返回true消息则被过滤
            return true;
        });
        return factory;
    }

    //消息过滤监听
    @KafkaListener(topics = {"topic1"},containerFactory = "filterContainerFactory")
    public void onMessage6(ConsumerRecord<?,?> record){
        System.out.println(record.value());
    }


    @KafkaListener(topics = {"hello2"})
    public void listen(ConsumerRecord<?, ?> record) {

        Optional.ofNullable(record.value())
                .ifPresent(message -> {
                    log.info("【+++++++++++++++++ record = {} 】", record);
                    log.info("【+++++++++++++++++ message = {}】", message);
                });
    }


    // 监听器用@KafkaListener注解，topics表示监听的topic，支持同时监听多个，用英文逗号分隔。启动项目，postman调接口触发生产者发送消息，
//    @KafkaListener(topics = {"topic-test-lzy"})
//    public void onMessage1(ConsumerRecord<?, ?> record) {
//        // 消费的哪个topic、partition的消息,打印出消息内容
//        System.out.println("简单消费Topic：" + record.topic() + "**分区" + record.partition() + "**值内容" + record.value());
//    }

    /**
     * @param record
     * @Title 指定topic、partition、offset消费
     * @Description 同时监听topic1和topic2，监听topic1的0号分区、
     * topic2的 "0号和1号" 分区，指向1号分区的offset初始值为8
     */
    @KafkaListener(id = "consumer1", groupId = "lzy-group", topicPartitions = {
            @TopicPartition(topic = "topic-test-lzy", partitions = {"0"}),
            @TopicPartition(topic = "topic2", partitions = "0"
                    //,partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "8")
            )
    })
    public void onMessage2(ConsumerRecord<?, ?> record) {
        System.out.println("-------------------------");
        System.out.println("topic:" + record.topic() + ",partition:" + record.partition() + ",offset:" + record.offset() + "value:" + record.value());
    }

//    /**
//     * 批量消费 （和普通消费只能2选1）
//     * @param records
//     */
//    @KafkaListener(id="consumer2",groupId = "felix-group",topics = "topic1" )
//    public void onMesssage(List<ConsumerRecord<?,?>> records){
//        System.out.println(">>>批量消费一次，records.size()="+records.size());
//        for(ConsumerRecord<?,?> record:records){
//            System.out.println(record.value());
//        }
//    }


// 将这个异常处理器的BeanName放到@KafkaListener注解的errorHandler属性里面
    @KafkaListener(topics = {"topic-test-lzy"},errorHandler = "consumerAwareErrorHandler")
    public void onMessage4(ConsumerRecord<?, ?> record) throws Exception {
        throw new Exception("简单消费-模拟异常");
    }

//    // 批量消费也一样，异常处理器的message.getPayload()也可以拿到各条消息的信息
//    @KafkaListener(topics = "topic1",errorHandler="consumerAwareErrorHandler")
//    public void onMessage5(List<ConsumerRecord<?, ?>> records) throws Exception {
//        System.out.println("批量消费一次...");
//        throw new Exception("批量消费-模拟异常");
//    }

    /**
     * @Title 消息转发
     * @Description 从topic1接收到的消息经过处理后转发到topic2
     * @param record
     * @return
     */
    @KafkaListener(topics = {"topic"})
    @SendTo("topic2")
    public String onMessage7(ConsumerRecord<?,?> record){
        return record.value()+"-forward message";
    }



}
