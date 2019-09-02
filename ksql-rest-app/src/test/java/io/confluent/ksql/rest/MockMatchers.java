package io.confluent.ksql.rest;

import org.apache.commons.io.IOUtils;
import org.hamcrest.CustomTypeSafeMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.hamcrest.integration.EasyMock2Adapter.adapt;

public class MockMatchers {
  public static InputStream inputStreamOf(String text) {
    final String description = "inputStreamOf(" + text + ")";
    adapt(new CustomTypeSafeMatcher<InputStream>(description) {
      @Override
      protected boolean matchesSafely(InputStream inputStream) {
        try {
          return text.equals(IOUtils.toString(inputStream, Charset.defaultCharset()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    return null;
  }
}
