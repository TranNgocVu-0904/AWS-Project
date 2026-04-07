package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;

import org.json.JSONObject;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class LambdaDownloadObject implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private static final Region REGION = Region.AP_SOUTHEAST_2;
    private static final String BUCKET = "public-cloud1";

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        try {
            String method = event != null && event.getRequestContext() != null
                    && event.getRequestContext().getHttp() != null
                    ? event.getRequestContext().getHttp().getMethod() : null;

            // GET /?key=... -> 302 redirect tới pre-signed URL
            if ("GET".equalsIgnoreCase(method)) {
                String key = event.getQueryStringParameters() != null
                        ? event.getQueryStringParameters().get("key") : null;
                if (key == null || key.isBlank()) 
                    return plain(400, "Missing 'key' in query");

                // check tồn tại
                S3Client s3 = S3Client.builder()
                        .region(REGION)
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .build();
                try {
                    s3.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(key).build());
                } catch (NoSuchKeyException e) {
                    return plain(404, "Not Found: " + key);
                }

                // presign 60s
                S3Presigner presigner = S3Presigner.builder().region(REGION).build();
                PresignedGetObjectRequest presigned = presigner.presignGetObject(
                        GetObjectPresignRequest.builder()
                                .signatureDuration(Duration.ofSeconds(60))
                                .getObjectRequest(r -> r.bucket(BUCKET).key(key))
                                .build()
                );

                Map<String, String> h = new HashMap<>();
                h.put("Location", presigned.url().toString());
                return APIGatewayV2HTTPResponse.builder()
                        .withStatusCode(302)
                        .withHeaders(h)
                        .build();
            }

            // PUT/POST body {"key": "..."} -> trả file (base64)
            if ("PUT".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method)) {
                String raw = event.getBody();
                if (raw == null || raw.isBlank()) return plain(400, "Missing body");
                if (Boolean.TRUE.equals(event.getIsBase64Encoded())) {
                    try { raw = new String(Base64.getDecoder().decode(raw)); } catch (IllegalArgumentException ignore) {}
                }

                JSONObject json;
                try { json = new JSONObject(raw); }
                catch (Exception e) { return plain(400, "Invalid JSON body"); }

                String key = json.optString("key", "").trim();
                if (key.isEmpty()) return plain(400, "Missing or empty 'key'");

                S3Client s3 = S3Client.builder()
                        .region(REGION)
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .build();

                String contentType;
                try {
                    HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(key).build());
                    contentType = head.contentType();
                } catch (NoSuchKeyException e) {
                    return plain(404, "Not Found: " + key);
                } catch (S3Exception e) {
                    return plain(502, "S3 headObject Error: " + e.awsErrorDetails().errorMessage());
                }
                if (contentType == null || contentType.isBlank()) contentType = "application/octet-stream";

                ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(BUCKET).key(key).build());

                String b64 = Base64.getEncoder().encodeToString(bytes.asByteArray());

                Map<String, String> h = new HashMap<>();
                h.put("content-type", contentType);
                String filename = key.substring(key.lastIndexOf('/') + 1);
                h.put("content-disposition", "inline; filename=\"" + filename + "\"");

                return APIGatewayV2HTTPResponse.builder()
                        .withStatusCode(200)
                        .withIsBase64Encoded(true) // API GW sẽ gửi bytes xuống client
                        .withHeaders(h)
                        .withBody(b64)
                        .build();
            }

            return plain(405, "Method Not Allowed");
        } catch (Exception e) {
            return plain(500, "Internal Error: " + e);
        }
    }

    private static APIGatewayV2HTTPResponse plain(int code, String msg) {
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(code)
                .withHeaders(Map.of("content-type", "text/plain; charset=utf-8"))
                .withBody(msg)
                .build();
    }
}