package com.cloudrangers.cloudpilotworker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
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
        return ExchangeBuilder.topicExchange(provisionExchangeName)
                .durable(true)
                .build();
    }

    @Bean("resultExchange")
    public TopicExchange resultExchange() {
        return ExchangeBuilder.topicExchange(resultExchangeName)
                .durable(true)
                .build();
    }

    // DLX도 실제로 하나 만들어 두는 게 안전
    @Bean("dlxExchange")
    public TopicExchange dlxExchange() {
        return ExchangeBuilder.topicExchange(dlxExchangeName)
                .durable(true)
                .build();
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

        return BindingBuilder.bind(queue)
                .to(exchange)
                .with(provisionRoutingKey);
    }

    @Bean
    public Binding resultBinding(
            @Qualifier("resultQueue") Queue queue,
            @Qualifier("resultExchange") TopicExchange exchange) {

        return BindingBuilder.bind(queue)
                .to(exchange)
                .with(resultRoutingKey);
    }

    // ============================
    // 1) RabbitTemplate → 결과 전송용 (JSON 사용)
    // ============================
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         ObjectMapper objectMapper) {

        RabbitTemplate template = new RabbitTemplate(connectionFactory);

        // 결과 메시지는 JSON으로 보내고 싶으니까 여기서는 Jackson 사용
        Jackson2JsonMessageConverter converter =
                new Jackson2JsonMessageConverter(objectMapper);
        template.setMessageConverter(converter);

        return template;
    }

    // ============================
    // 2) Listener 전용 컨버터 → raw payload만 받도록
    // ============================
    @Bean("workerListenerMessageConverter")
    public MessageConverter workerListenerMessageConverter() {
        // 🔥 중요: Jackson 말고 SimpleMessageConverter 사용
        // → __TypeId__ 헤더를 전혀 보지 않음
        // → payload 는 byte[] / String 으로만 다룸
        return new SimpleMessageConverter();
    }

    // ============================
    // 3) Listener Container Factory
    // ============================
    @Bean("rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            @Qualifier("workerListenerMessageConverter")
            MessageConverter listenerMessageConverter
    ) {
        SimpleRabbitListenerContainerFactory factory =
                new SimpleRabbitListenerContainerFactory();

        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(listenerMessageConverter);

        // 실패 시 재큐잉 금지 → DLX / drop
        factory.setDefaultRequeueRejected(false);
        // 큐 없다고 애플리케이션 죽지 않게
        factory.setMissingQueuesFatal(false);

        // 필요하면 동시 소비자 수 조절
        // factory.setConcurrentConsumers(1);
        // factory.setMaxConcurrentConsumers(1);

        return factory;
    }

    // ===== Admin (자동 선언 ON) =====
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory cf) {
        RabbitAdmin admin = new RabbitAdmin(cf);
        admin.setAutoStartup(true);
        return admin;
    }
}
