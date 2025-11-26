package com.cloudrangers.cloudpilotworker.config;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

@Configuration
public class RabbitConfig {

    // ===== 이름/키 =====
    @Value("${rabbitmq.exchange.provision.name}") private String provisionExchangeName;
    @Value("${rabbitmq.exchange.result.name}")    private String resultExchangeName;
    @Value("${rabbitmq.exchange.dlx.name}")       private String dlxExchangeName;

    @Value("${rabbitmq.queue.provision.name}")    private String provisionQueueName;
    @Value("${rabbitmq.queue.result.name}")       private String resultQueueName;

    @Value("${rabbitmq.routing-key.provision}")   private String provisionRoutingKey;
    @Value("${rabbitmq.routing-key.result}")      private String resultRoutingKey;
    @Value("${rabbitmq.routing-key.dlq}")         private String dlqRoutingKey;

    // ===== Exchange =====
    @Bean("provisionExchange")
    public TopicExchange provisionExchange() {
        return ExchangeBuilder.topicExchange(provisionExchangeName).durable(true).build();
    }

    @Bean("resultExchange")
    public TopicExchange resultExchange() {
        return ExchangeBuilder.topicExchange(resultExchangeName).durable(true).build();
    }

    // ===== Queue =====
    @Bean("provisionQueue")
    public Queue provisionQueue() {
        return QueueBuilder.durable(provisionQueueName)
                .withArgument("x-dead-letter-exchange", dlxExchangeName)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    @Bean("resultQueue")
    public Queue resultQueue() {
        return QueueBuilder.durable(resultQueueName).build();
    }

    // ===== Binding =====
    @Bean
    public Binding provisionBinding(
            @Qualifier("provisionQueue") Queue queue,
            @Qualifier("provisionExchange") TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(provisionRoutingKey);
    }

    @Bean
    public Binding resultBinding(
            @Qualifier("resultQueue") Queue queue,
            @Qualifier("resultExchange") TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(resultRoutingKey);
    }

    // ===== Converter (⭐️ TypeId 완전히 무시) =====
    @Bean
    public MessageConverter messageConverter(ObjectMapper objectMapper) {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);

        // ⭐️ 커스텀 TypeMapper: __TypeId__ 헤더를 완전히 무시
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper() {
            @Override
            public JavaType toJavaType(org.springframework.amqp.core.MessageProperties properties) {
                // __TypeId__ 헤더를 무시하고 기본 타입(Object) 반환
                return TypeFactory.defaultInstance().constructType(Object.class);
            }
        };

        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    // ===== Message Handler Method Factory (⭐️ 변환 비활성화) =====
    @Bean
    public MessageHandlerMethodFactory messageHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        // 메시지 변환을 건너뛰고 원시 Message를 전달
        return factory;
    }

    // ===== Listener Factory =====
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter
    ) {
        SimpleRabbitListenerContainerFactory f = new SimpleRabbitListenerContainerFactory();
        configurer.configure(f, connectionFactory);

        // ⭐️ 커스텀 MessageConverter 설정
        f.setMessageConverter(messageConverter);

        // 실패 시 재큐잉 금지 → DLX로 바로 이동
        f.setDefaultRequeueRejected(false);
        // 큐가 잠시 없어도 기동
        f.setMissingQueuesFatal(false);

        return f;
    }

    // ===== Admin (자동 선언 ON) =====
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory cf) {
        RabbitAdmin admin = new RabbitAdmin(cf);
        admin.setAutoStartup(true);
        return admin;
    }
}