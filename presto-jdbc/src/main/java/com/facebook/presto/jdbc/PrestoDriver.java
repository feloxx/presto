/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.jdbc;

import com.facebook.presto.client.SocketChannelSocketFactory;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.client.OkHttpUtil.userAgent;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Integer.parseInt;

/**
 * presto jdbc client 启动类
 * 继承了 java.sql.Driver 接口,所以我们可以像大部分jdbc包那样,引入它,然后配置一个jdbc连接driver和url即可连接
 * 一个Driver的流程为
 *
 * connect           创建连接
 * acceptsURL        校验url
 * getPropertyInfo   获得配置
 *
 * getMajorVersion
 * getMinorVersion
 * jdbcCompliant
 *
 * getParentLogger
 */

public class PrestoDriver
        implements Driver, Closeable
{
    static final String DRIVER_NAME = "Presto JDBC Driver";
    static final String DRIVER_VERSION;
    static final int DRIVER_VERSION_MAJOR;
    static final int DRIVER_VERSION_MINOR;

    private static final String DRIVER_URL_START = "jdbc:presto:";

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .addInterceptor(userAgent(DRIVER_NAME + "/" + DRIVER_VERSION))
            .socketFactory(new SocketChannelSocketFactory())
            .build();

    static {
        String version = nullToEmpty(PrestoDriver.class.getPackage().getImplementationVersion());
        Matcher matcher = Pattern.compile("^(\\d+)\\.(\\d+)($|[.-])").matcher(version);
        if (!matcher.find()) {
            DRIVER_VERSION = "unknown";
            DRIVER_VERSION_MAJOR = 0;
            DRIVER_VERSION_MINOR = 0;
        }
        else {
            DRIVER_VERSION = version;
            DRIVER_VERSION_MAJOR = parseInt(matcher.group(1));
            DRIVER_VERSION_MINOR = parseInt(matcher.group(2));
        }

        try {
            DriverManager.registerDriver(new PrestoDriver());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    //- [v236][client jdbc][001][start] 开始创建一个新的jdbc连接
    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        //- [v236][client jdbc][002] 解析校验 jdbc url 和 账号密码
        PrestoDriverUri uri = new PrestoDriverUri(url, info);

        OkHttpClient.Builder builder = httpClient.newBuilder();
        uri.setupClient(builder);
        QueryExecutor executor = new QueryExecutor(builder.build());

        //- [v236][client jdbc][003] 创建 presto 连接
        return new PrestoConnection(uri, executor);
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        Properties properties = new PrestoDriverUri(url, info).getProperties();

        return ConnectionProperties.allProperties().stream()
                .map(property -> property.getDriverPropertyInfo(properties))
                .toArray(DriverPropertyInfo[]::new);
    }

    @Override
    public int getMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }
}
