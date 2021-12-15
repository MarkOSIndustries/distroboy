package distroboy.example.localstack;

import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * This is necessary because the Java AWS SDK doesn't append a trailing slash when performing
 * actions directly against the bucket, like listing keys in a bucket. Unfortunately, localstack
 * doesn't know which service to send a request to without that trailing slash, so we add it in when
 * dealing with localstack.
 */
public class TempSdkHttpClientTrailingSlashAppender implements SdkHttpClient {
  private final SdkHttpClient inner;

  public TempSdkHttpClientTrailingSlashAppender(SdkHttpClient inner) {
    this.inner = inner;
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    if ("/distroboy-bucket".equals(request.httpRequest().encodedPath())) {
      final var fakeReq =
          HttpExecuteRequest.builder()
              .request(
                  request
                      .httpRequest()
                      .copy(b -> b.encodedPath(request.httpRequest().encodedPath() + "/").build()))
              .build();
      return inner.prepareRequest(fakeReq);
    }
    return inner.prepareRequest(request);
  }

  @Override
  public void close() {}
}
