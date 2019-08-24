package com.subrato.packages.solace.MessagingAppWithQueuePesistance.config;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;

import java.util.concurrent.CountDownLatch;

public class PublisherCallback implements JCSMPStreamingPublishCorrelatingEventHandler {

    private CountDownLatch latch = null;

    public PublisherCallback(CountDownLatch latch){
        this.latch = latch;
    }

    @Override
    public void responseReceivedEx(Object key) {
        if (key instanceof MessageInfo) {
            MessageInfo msgInfo = (MessageInfo) key;
            msgInfo.acked = true;
            msgInfo.publishedSuccessfully = true;
            System.out.printf("Message response (accepted) received for %s \n", msgInfo);
        }
        latch.countDown();
    }

    @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
        if (key instanceof MessageInfo) {
            MessageInfo msgInfo = (MessageInfo) key;
            msgInfo.acked = true;
            System.out.printf("Message response (rejected) received for %s, error was %s \n", msgInfo, cause);
        }
        latch.countDown();
    }

    @Override
    public void handleError(String s, JCSMPException e, long l) {

    }

    @Override
    public void responseReceived(String s) {

    }
}
