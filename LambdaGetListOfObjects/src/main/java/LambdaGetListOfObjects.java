package vgu.cloud26;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class LambdaGetListOfObjects implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        context.getLogger().log("Received request: " + request.getBody());

        String bucketName = "public-cloud1";

        S3Client s3Client = S3Client.builder()
                //.credentialsProvider(InstanceProfileCredentialsProvider.create())
                .region(Region.AP_SOUTHEAST_2)
                .build();
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build();

        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        JSONArray objArray = new JSONArray();

        for (S3Object object : objects) {
            JSONObject obj = new JSONObject();
            obj.put("key", object.key());
            obj.put("size", calKb(object.size()));
            objArray.put(obj);

        }

        APIGatewayProxyResponseEvent response
                = new APIGatewayProxyResponseEvent();
        response.setStatusCode(200);
        response.setBody(objArray.toString());
        response.setHeaders(java.util.Collections.singletonMap("Content-Type", "application/json"));
        return response;
    }
    //convert bytes to kbs.
    private static long calKb(Long val) {
        return val / 1024;
    }
}