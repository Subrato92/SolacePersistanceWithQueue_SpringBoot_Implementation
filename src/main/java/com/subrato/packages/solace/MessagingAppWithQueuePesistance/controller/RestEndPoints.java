package com.subrato.packages.solace.MessagingAppWithQueuePesistance.controller;

import com.solacesystems.jcsmp.JCSMPException;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.config.MessageRouter;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.config.Producer;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.config.Subscriber;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.pojos.InitializerPayload;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.pojos.StringPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.concurrent.CountDownLatch;

@RestController
@EnableSwagger2
@RequestMapping(value = "/solace/queue")
public class RestEndPoints {

    private Producer producer = null;
    private Subscriber subscriber = null;
    private MessageRouter router = null;
    private CountDownLatch latch = null;
    private Logger log = LoggerFactory.getLogger(RestEndPoints.class);

    @PostMapping(
            value = "/initialize",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public @ResponseBody StringPayload initialize(@RequestBody InitializerPayload payload) {

        //Initializing Message Router
        router = new MessageRouter(payload);
        String routerResp = router.connect();
        log.info("[INITIALIZE] Router Response : " + routerResp);

        latch = new CountDownLatch(1);

        //Initializing Producer
        producer = new Producer();
        try {
            producer.initialize(router.getSession(), latch);
            log.info("[INITIALIZE] Producer Initialized.");
        } catch (JCSMPException e) {
            log.info("[INITIALIZE] Producer Initialization Failed - " + e.getMessage());
        }

        //Initializing Subscriber
        subscriber = new Subscriber();
        try {
            subscriber.subscribe(router.getQueue(), router.getSession(), latch);
            log.info("[INITIALIZE] Subscriber Initialized.");
        } catch (JCSMPException e) {
            log.info("[INITIALIZE] Subscriber Initialization Failed - " + e.getMessage());
        }

        return new StringPayload("Initialized Method Called");
    }

    @GetMapping(
            value = "/getMsg",
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public @ResponseBody StringPayload getMsg(){

        if(subscriber != null){
            return new StringPayload(subscriber.getMesssages());
        }

        return new StringPayload("Subscriber Is Not Initialized. Pls Run '/solace/queue/initialize'");
    }

    @PostMapping(
            value = "/postmsg",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public @ResponseBody StringPayload postMessages(@RequestBody StringPayload message){
        String response = null;

        if( producer!=null ){
            response = producer.sendMsg(message.getPayload(), router.getQueue());
        }else{
            response = "Producer Not Initialized";
        }

        return new StringPayload(response);
    }

    @GetMapping(value = "/reconcile")
    public @ResponseBody StringPayload reconsile(){
        return new StringPayload(subscriber.getMesssages());
    }

}
