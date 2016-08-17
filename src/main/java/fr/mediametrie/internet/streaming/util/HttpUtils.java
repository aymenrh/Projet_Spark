package fr.mediametrie.internet.streaming.util;

import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

/**
 * Provide commons methods to run HTTP requests.
 */
public class HttpUtils {

    private static int MAX_RETRY = 3;

    private static final HttpClient client = new HttpClient();

    /**
     * Run a GET HTTP request on the given url and return the response as a String.
     *
     * @return the response as a String or null when the STATUS is not 200.
     * @throws IOException
     */
    public static String get(String url) throws IOException {
        String httpResult;

        GetMethod method = new GetMethod(url);

        // Fixed retry : 3
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(MAX_RETRY, false));

        // Execute the method.
        int statusCode = client.executeMethod(method);

        if (statusCode != HttpStatus.SC_OK) {
            return null;
        }

        httpResult = new String(method.getResponseBody());

        method.releaseConnection();
        return httpResult;
    }

}
