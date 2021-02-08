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
package com.facebook.presto.client;

import okhttp3.OkHttpClient;

public final class StatementClientFactory
{
    private StatementClientFactory() {}

    public static StatementClient newStatementClient(OkHttpClient httpClient, ClientSession session, String query)
    {
        //- [v236][client sql][012] 创建一个新的查询实例3，这里还没明白为什么要嵌套怎么多层, 可能是为了照顾多种客户端的使用,比如console client 或 jdbc client
        return new StatementClientV1(httpClient, session, query);
    }
}
