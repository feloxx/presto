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
package com.facebook.presto.cli;

import static io.airlift.airline.SingleCommand.singleCommand;

public final class Presto
{
    private Presto() {}

    public static void main(String[] args)
    {
        Console console = singleCommand(Console.class).parse(args);

        if (console.helpOption.showHelpIfRequested() ||
                console.versionOption.showVersionIfRequested()) {
            return;
        }

        // [code-read][v236][001][client entry] client入口，启动一个console，返回一个bool ture为正常退出，false为非正常退出
        System.exit(console.run() ? 0 : 1);
    }
}
