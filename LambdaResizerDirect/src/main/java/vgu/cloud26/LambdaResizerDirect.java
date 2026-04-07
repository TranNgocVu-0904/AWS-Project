package vgu.cloud26;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import org.json.JSONObject;

public class LambdaResizerDirect implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // --- Configuration Constants ---
    private static final float MAX_DIMENSION = 100; // Max width/height for the thumbnail
    private final String REGEX = ".*\\.([^\\.]*)"; // Regex to extract file extension
    private final String JPG_TYPE = "jpg";
    private final String JPEG_TYPE = "jpeg";
    private final String JPG_MIME = "image/jpeg";
    private final String PNG_TYPE = "png";
    private final String PNG_MIME = "image/png";

    // Target Bucket for Thumbnails
    private final String DEST_BUCKET_NAME = "resized-public-cloud1";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();

        try {
            String requestBody = request.getBody();

            JSONObject body = new JSONObject(requestBody);

            // 1. PARSE INPUT DATA
            // content: Base64 string of the image
            // key: Original filename (e.g., "vacation.jpg")
            // email: User's email (used for folder structure)
            String content = body.getString("content");
            String key     = body.getString("key");
            String email   = body.optString("email", "unknown_user"); 

            // 2. VALIDATE IMAGE TYPE
            // We only support JPG and PNG
            String imageType = getImageType(key);
            
            if (imageType == null) {
                logger.log("Unable to infer image type for key " + key);
                throw new IllegalArgumentException("File is not an image (cannot infer extension).");
            }

            if (!(JPG_TYPE.equalsIgnoreCase(imageType))
                && !(PNG_TYPE.equalsIgnoreCase(imageType))
                && !JPEG_TYPE.equalsIgnoreCase(imageType)) {
                logger.log("Skipping non-image " + key);
                throw new IllegalArgumentException("File type '" + imageType + "' is not supported.");
            }

            // 3. CONSTRUCT S3 PATH (HASHING LOGIC)
            // This logic must match 'LambdaUploadObject' to ensure consistency.
            // Target format: "email/resized-<HASH>.jpg"
            
            int lastDotIndex = key.lastIndexOf('.');
            String namePart;
            String extensionPart;

            if (lastDotIndex == -1) {
                namePart = key;
                extensionPart = ""; 
            } else {
                namePart = key.substring(0, lastDotIndex);
                extensionPart = key.substring(lastDotIndex); // e.g., ".jpg"
            }

            // Calculate SHA-256 Hash of the filename
            String hashValue = sha256Hex(namePart);

            // Build the final path with "resized-" prefix
            String finalResizedKey = email + "/resized-" + hashValue + extensionPart;

            logger.log("Resizing: " + key + " -> " + finalResizedKey);

            // 4. IMAGE PROCESSING (Decode -> Resize -> Encode)
            
            // a. Decode Base64 string to Byte Array
            byte[] objBytes = Base64.getDecoder().decode(content.getBytes());

            // b. Read Bytes into a BufferedImage object
            BufferedImage srcImage = ImageIO.read(new ByteArrayInputStream(objBytes));
            if (srcImage == null) {
                throw new IllegalArgumentException("Uploaded content is not a valid image.");
            }

            // c. Perform Resizing (using helper method)
            BufferedImage resizedImage = resizeImage(srcImage);

            // d. Encode resized image back to Byte Array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(resizedImage, imageType, outputStream);
            byte[] resizedBytes = outputStream.toByteArray();

            // 5. UPLOAD THUMBNAIL TO S3
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(DEST_BUCKET_NAME)
                .key(finalResizedKey) // Use the new hashed path
                .metadata(buildMetadata(resizedBytes.length, imageType))
                .build();

            S3Client s3Client = S3Client.builder()
                .region(Region.AP_SOUTHEAST_2)
                .build();

            // Execute Upload
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(resizedBytes));
            logger.log("Object successfully resized to " + DEST_BUCKET_NAME + "/" + finalResizedKey);

            // 6. RETURN SUCCESS RESPONSE
            JSONObject respJson = new JSONObject();
            respJson.put("message", "Object resized and uploaded successfully");
            respJson.put("originalKey", key);          
            respJson.put("resizedKey", finalResizedKey); // Return the path so UI can find it

            APIGatewayProxyResponseEvent resp = new APIGatewayProxyResponseEvent();
            resp.setStatusCode(200);
            resp.setBody(respJson.toString());
            resp.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            return resp;

        } catch (IllegalArgumentException e) {
            logger.log("Bad request: " + e.getMessage());
            JSONObject respJson = new JSONObject();
            respJson.put("message", e.getMessage());
            APIGatewayProxyResponseEvent resp = new APIGatewayProxyResponseEvent();
            resp.setStatusCode(400);
            resp.setBody(respJson.toString());
            resp.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            return resp;

        } catch (Exception e) {
            logger.log("Error: " + e.toString());
            APIGatewayProxyResponseEvent resp = new APIGatewayProxyResponseEvent();
            resp.setStatusCode(500);
            resp.setBody("{\"message\":\"Error: " + e.getMessage() + "\"}");
            resp.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            return resp;
        }
    }

    // ======================= HELPER: HASHING =======================
    /**
     * Standard SHA-256 Hashing function.
     * Used to ensure the thumbnail has the same hash ID as the original file.
     */
    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
            
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }
    }

    // ======================= IMAGE PROCESSING HELPERS =======================

    /**
     * Extracts the file extension from the key.
     */
    private String getImageType(String srcKey) {
        Matcher matcher = Pattern.compile(REGEX).matcher(srcKey);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(1).toLowerCase();
    }

    /**
     * Creates metadata (Content-Type, Content-Length) for S3 Object.
     */
    private Map<String, String> buildMetadata(int length, String imageType) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("Content-Length", Integer.toString(length));

        if (JPG_TYPE.equalsIgnoreCase(imageType) || JPEG_TYPE.equalsIgnoreCase(imageType)) {
            metadata.put("Content-Type", JPG_MIME);
        } 
        else if (PNG_TYPE.equalsIgnoreCase(imageType)) {
            metadata.put("Content-Type", PNG_MIME);
        }
        return metadata;
    }

    /**
     * Resizes the image while maintaining aspect ratio using standard Java AWT.
     */
    private BufferedImage resizeImage(BufferedImage srcImage) {
        int srcHeight = srcImage.getHeight();
        int srcWidth = srcImage.getWidth();
        
        // Calculate the scaling factor to fit within MAX_DIMENSION (100px)
        float scalingFactor = Math.min(
                MAX_DIMENSION / srcWidth, MAX_DIMENSION / srcHeight);
        int width = (int) (scalingFactor * srcWidth);
        int height = (int) (scalingFactor * srcHeight);

        // Create a new, blank image with the new dimensions
        BufferedImage resizedImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = resizedImage.createGraphics();
        
        // Paint the background white (handles transparency issues in PNG -> JPG conversion)
        graphics.setPaint(Color.white);
        graphics.fillRect(0, 0, width, height);
        
        // Apply Bilinear Interpolation for smooth resizing
        graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        
        // Draw the original image onto the new smaller canvas
        graphics.drawImage(srcImage, 0, 0, width, height, null);
        graphics.dispose();
        return resizedImage;
    }
}