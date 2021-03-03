package com.wind.datax;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
//import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@ComponentScan(basePackages = {"com.wind"})
@EnableDiscoveryClient
@EnableFeignClients
public class DataxApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataxApplication.class,args);
        System.out.println("=^_^= 溫馨的微笑!!! \n" +
                             "Y(^_^)Y 舉雙手勝利\n" +
                                   "\\^o^/ 歡呼\n");
    }
}
