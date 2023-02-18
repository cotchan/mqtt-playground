package com.example.publisher;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class Publisher {

    @Value("${mqtt.broker.host}")
    private String host;

    @Value("${mqtt.broker.port}")
    private int port;

    @Value("${mqtt.broker.username}")
    private String userName;

    @Value("${mqtt.broker.password}")
    private String password;

    private Mqtt5BlockingClient client;

    public Publisher() {
    }

    @PostConstruct
    public void connect() throws InterruptedException {
        client = MqttClient.builder()
                .useMqttVersion5()
                .serverHost(host)
                .serverPort(port)
                .sslWithDefaultConfig()
                .buildBlocking();

        // connect to HiveMQ Cloud with TLS and username/pw
        client.connectWith()
                .simpleAuth()
                .username(userName)
                .password(UTF_8.encode(password))
                .applySimpleAuth()
                .send();

        System.out.println("Publisher::Connected successfully");

        int loopCnt = 0;

        while (loopCnt != 100) {
            // publish a message to the topic "my/test/topic"
            client.publishWith()
                    .topic("my/test/topic")
                    .payload(UTF_8.encode("Hello " + ++loopCnt))
                    .send();

            TimeUnit.MILLISECONDS.sleep(2000);
            System.out.println("publishWith loopCnt = " + loopCnt);
        }
    }
}
