# Presto 源码阅读

对presto源码阅读的分享，此分享基于presto 0.236.1分支。

并以直接对代码中假如注释来标记阅读流程。

## 源码阅读格式

主要分为两个格式：

- //* 零散的注释，标记简单功能

例子：

```
//* 实例化一个console对象，并解析输入参数
```

- //- \[版本号]\[流程名]\[流程序号]\[可选的开始或结束标记]  有流程的阅读，标记一条流程，开始与结束都会带上一个标记

例子：

```
//- [v236][client sql][001][start] client入口，启动一个console，返回一个bool ture为正常退出，false为非正常退出

...

//- [v236][client sql][013] 开始构建一个请求

...

//- [v236][client sql][014][end] 等待响应，这个地方是同步的？

```

## 分享形式

每阅读完一个关键流程，或者一些重要的地方，会形成文档。

- console 客户端sql提交流程
- jdbc 客户端sql提交流程,`//- [v236][client jdbc][001][start] 开始创建一个新的jdbc连接`
- 服务端sql接收流程
