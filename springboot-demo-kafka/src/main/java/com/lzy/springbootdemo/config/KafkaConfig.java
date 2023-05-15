package com.lzy.springbootdemo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic initTopic(){
        return new NewTopic("topic-test-lzy",3,(short)2);
    }

    @Bean
    public NewTopic updataTopic(){
        return new NewTopic("topic-test-lzy-1",4,(short) 2);
    }

    // 新建一个异常处理器，用@Bean注入
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
        return (message, exception, consumer) -> {
            System.out.println("消费异常："+message.getPayload());
            return null;
        };
    }

}
