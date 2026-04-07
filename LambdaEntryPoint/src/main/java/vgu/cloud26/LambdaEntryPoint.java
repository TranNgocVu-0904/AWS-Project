package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.json.JSONObject;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;


public class LambdaEntryPoint implements
        RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private final LambdaClient lambdaClient;
       
    public LambdaEntryPoint() {
        this.lambdaClient = LambdaClient.builder()
                .region(Region.of("ap-southeast-2"))
                .build();
    }
    
    public String callLambda(String functionName, String payload,  LambdaLogger logger) {
        String message;
        InvokeRequest invokeRequest = InvokeRequest.builder()
                .functionName(functionName)
                //.invocationType("Event") // Asynchronous invocation
                .payload(SdkBytes.fromUtf8String(payload))
                .invocationType("RequestResponse")
                .build();

        try {
            InvokeResponse invokeResult = 
                    lambdaClient.invoke(invokeRequest);
      
            ByteBuffer responsePayload = 
                    invokeResult.payload().asByteBuffer();
            String responseString = StandardCharsets.UTF_8.decode(responsePayload).toString();

            JSONObject responseObject = new JSONObject(responseString);
            message = responseObject.getString("body");
            logger.log("Response: " + message);
                          
            return message;
        } catch (AwsServiceException | SdkClientException e) {
            message = "Error " + functionName + 
                    ": " + e.getMessage();
            logger.log(message);
        }
        return message;
    }
    
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context cntxt) {
        
        LambdaLogger logger = cntxt.getLogger();
        logger.log("Invoking");
        LambdaEntryPoint caller = new LambdaEntryPoint();
        JSONObject body = new JSONObject();
        body.put("key", "indexx.html");
        JSONObject json = new JSONObject();
        json.put("body", body.toString());
        String payload = json.toString();
        String message = caller.callLambda("LambdaShowUpObject", 
                payload, logger);

       
        Map<String, String> headersMap;
            headersMap = Map.of(
                    "content-type", "text/html");

            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(200)
                    .withHeaders(headersMap)
                    .withBody(message)
                    .withIsBase64Encoded(true);

    }

}