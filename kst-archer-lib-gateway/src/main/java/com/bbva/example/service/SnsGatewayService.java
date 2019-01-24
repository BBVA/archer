package com.bbva.example.service;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.bbva.archer.avro.gateway.example.OutputEvent;
import com.bbva.common.consumers.CRecord;
import com.bbva.gateway.service.impl.GatewayService;

//@ServiceConfig(file = "example/services/sns.yml")
public class SnsGatewayService extends GatewayService<PublishResult> {
    private AmazonSNSClient snsClient;
    private String topicArn;

    @Override
    public PublishResult call(final CRecord record) {
        final String msg = record.value().toString();
        final PublishRequest publishRequest = new PublishRequest(topicArn, msg);

        return new PublishResult().withMessageId("msgid");//snsClient.publish(publishRequest);
    }

    @Override
    protected Boolean isSuccess(final PublishResult result) {
        return result.getMessageId() != null;
    }

    @Override
    public void postInitActions() {
        /*final Map gatewayConfig = config.getGateway();
        snsClient = new AmazonSNSClient(new BasicAWSCredentials((String) gatewayConfig.get("accessKey"),
                (String) gatewayConfig.get("secretKey")));
        topicArn = (String) gatewayConfig.get("arn");
        snsClient.setRegion(Region.getRegion(Regions.EU_WEST_1));

        final SubscribeRequest subRequest = new SubscribeRequest(topicArn, (String) gatewayConfig.get("subscriptionType"),
                (String) gatewayConfig.get("subscriptionValue"));
        snsClient.subscribe(subRequest);*/
    }

    @Override
    public void processResult(final CRecord originRecord, final PublishResult result) {
        sendEvent(originRecord, new OutputEvent("sns-" + result.getMessageId()));
    }
}
