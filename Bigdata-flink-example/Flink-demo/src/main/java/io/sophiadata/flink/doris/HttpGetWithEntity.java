package io.sophiadata.flink.doris;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;

/** (@SophiaData) (@date 2022/11/15 09:07). */
public class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {
    private static final String METHOD_NAME = "GET";

    @Override
    public String getMethod() {
        return METHOD_NAME;
    }

    public HttpGetWithEntity(final String uri) {
        super();
        setURI(URI.create(uri));
    }
}
