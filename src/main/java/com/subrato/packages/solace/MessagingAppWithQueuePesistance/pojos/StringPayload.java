package com.subrato.packages.solace.MessagingAppWithQueuePesistance.pojos;

public class StringPayload {

    private String payload;

    public StringPayload(String payload){
        this.payload = payload;
    }

    public StringPayload(){
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

}
