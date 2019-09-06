package io.confluent.ksql.rest;

import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class MockMatchers {
  public static InputStream inputStreamOf(String text) {
    EasyMock.reportMatcher(new IArgumentMatcher() {
      @Override
      public boolean matches(Object o) {
        try {
          return text.equals(IOUtils.toString((InputStream) o, Charset.defaultCharset()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void appendTo(StringBuffer stringBuffer) {
        stringBuffer.append("inputStreamOf(").append(text).append(")");
      }
    });
    return null;
  }
}
