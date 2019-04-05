/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.client;

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Map;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import io.confluent.rest.RestConfig;
import javax.net.ssl.TrustManagerFactory;

import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.util.StringUtil;

public class SslContextFactory {
  private String protocol;
  private String provider;
  private String kmfAlgorithm;
  private String tmfAlgorithm;
  private SecurityStore keystore = null;
  private String keyPassword;
  private SecurityStore truststore;
  private SSLContext sslContext;

  public SslContextFactory(final Map<?, ?> props) throws KsqlRestClientException {
    final RestConfig configs = new RestConfig(RestConfig.baseConfigDef(), props);
    this.protocol = configs.getString(RestConfig.SSL_PROTOCOL_CONFIG);
    this.provider = configs.getString(RestConfig.SSL_PROVIDER_CONFIG);

    this.kmfAlgorithm = configs.getString(RestConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG);
    this.tmfAlgorithm = configs.getString(RestConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

    createKeystore(
            configs.getString(RestConfig.SSL_KEYSTORE_TYPE_CONFIG),
            configs.getString(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG),
            configs.getPassword(RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG).toString(),
            configs.getPassword(RestConfig.SSL_KEY_PASSWORD_CONFIG).toString()
    );

    createTruststore(
            configs.getString(RestConfig.SSL_TRUSTSTORE_TYPE_CONFIG),
            configs.getString(RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG),
            configs.getPassword(RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString()
    );
    try {
      this.sslContext = createSslContext();
    } catch (Exception e) {
      throw new KsqlRestClientException("Error initializing the ssl context for RestService", e);
    }
  }


  private SSLContext createSslContext() throws GeneralSecurityException, IOException {
    final SSLContext sslContext;
    if (StringUtil.isNotBlank(provider)) {
      sslContext = SSLContext.getInstance(protocol, provider);
    } else {
      sslContext = SSLContext.getInstance(protocol);
    }

    KeyManager[] keyManagers = null;
    if (keystore != null) {
      final String kmfAlgorithm =
              StringUtil.isNotBlank(this.kmfAlgorithm) ? this.kmfAlgorithm
                      : KeyManagerFactory.getDefaultAlgorithm();
      final KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
      final KeyStore ks = keystore.load();
      final String keyPassword = this.keyPassword != null ? this.keyPassword : keystore.password;
      kmf.init(ks, keyPassword.toCharArray());
      keyManagers = kmf.getKeyManagers();
    }

    final String tmfAlgorithm =
            StringUtil.isNotBlank(this.tmfAlgorithm) ? this.tmfAlgorithm
                    : TrustManagerFactory.getDefaultAlgorithm();
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
    final KeyStore ts = truststore == null ? null : truststore.load();
    tmf.init(ts);

    sslContext.init(keyManagers, tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }


  /**
   * Returns a configured SSLContext.
   *
   * @return SSLContext.
   */
  public SSLContext sslContext() {
    return sslContext;
  }

  private void createKeystore(final String type, final String path,
                              final String password, final String keyPassword)
          throws KsqlRestClientException {
    if (path == null && password != null) {
      throw new KsqlRestClientException(
              "SSL key store is not specified, but key store password is specified.");
    } else if (path != null && password == null) {
      throw new KsqlRestClientException(
              "SSL key store is specified, but key store password is not specified.");
    } else if (StringUtil.isNotBlank(path) && StringUtil.isNotBlank(password)) {
      this.keystore = new SecurityStore(type, path, password);
      this.keyPassword = keyPassword;
    }
  }

  private void createTruststore(String type, String path, String password)
          throws KsqlRestClientException {

    if (path == null && password != null) {
      throw new KsqlRestClientException(
              "SSL trust store is not specified, but trust store password is specified.");
    }

    final boolean secureCluster = UserGroupInformation.isSecurityEnabled();
    if (secureCluster && (path == null || path.isEmpty())) {
      try (SslConfig sslConfig = WebSecurityManager
              .getSslConfig(SslConfig.SslConfigScope.SCOPE_CLIENT_ONLY)) {
        path = sslConfig.getClientTruststoreLocation();
        password = new String(sslConfig.getClientTruststorePassword());
      }
    }

    if (StringUtil.isNotBlank(path)) {
      this.truststore = new SecurityStore(type, path, password);
    }

  }

  private static final class SecurityStore {

    private final String type;
    private final String path;
    private final String password;

    private SecurityStore(final String type, final String path, final String password) {
      this.type = type == null ? KeyStore.getDefaultType() : type;
      this.path = path;
      this.password = password;
    }

    private KeyStore load() throws GeneralSecurityException, IOException {
      FileInputStream in = null;
      try {
        final KeyStore ks = KeyStore.getInstance(type);
        in = new FileInputStream(path);
        final char[] passwordChars = password != null ? password.toCharArray() : null;
        ks.load(in, passwordChars);
        return ks;
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
  }
}
