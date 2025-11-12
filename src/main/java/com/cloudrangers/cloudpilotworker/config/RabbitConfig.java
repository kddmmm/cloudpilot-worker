package com.cloudrangers.cloudpilotworker.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    // ===== 이름/키 =====
    @Value("${rabbitmq.exchange.provision.name}") private String provisionExchangeName;
    @Value("${rabbitmq.exchange.result.name}")    private String resultExchangeName;
    @Value("${rabbitmq.exchange.dlx.name}")       private String dlxExchangeName;

    @Value("${rabbitmq.queue.provision.name}")    private String provisionQueueName;
    @Value("${rabbitmq.queue.result.name}")       private String resultQueueName;

    @Value("${rabbitmq.routing-key.provision}")   private String provisionRoutingKey;   // e.g. "provision.#"
    @Value("${rabbitmq.routing-key.result}")      private String resultRoutingKey;      // e.g. "result.provision.#"
    @Value("${rabbitmq.routing-key.dlq}")         private String dlqRoutingKey;         // e.g. "provision.dlq"

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
    // 현재 브로커의 provision-jobs 큐에는 DLX 인자가 있으므로, 워커도 동일하게 선언
    @Bean("provisionQueue")
    public Queue provisionQueue() {
        return QueueBuilder.durable(provisionQueueName)
                .withArgument("x-dead-letter-exchange", dlxExchangeName)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)  // "provision.dlq"
                .build();
    }

    @Bean("resultQueue")
    public Queue resultQueue() {
        return QueueBuilder.durable(resultQueueName).build();
    }

    // ===== Binding =====
    // 파라미터 주입 방식으로 동일 빈을 안전하게 참조
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

    // ===== Converter =====
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ===== Listener Factory =====
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory
    ) {
        SimpleRabbitListenerContainerFactory f = new SimpleRabbitListenerContainerFactory();
        configurer.configure(f, connectionFactory);
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
        admin.setAutoStartup(true); // 브로커에 동일 자원이 없으면 이 설정으로 선언됨
        return admin;
    }
}
