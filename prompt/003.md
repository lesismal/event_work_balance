### 001

按照现在c语言版的epoll网络库的架构设计，实现一个golang版的。


### 002

golang版本逻辑协程池改为用参考 github.com/lesismal/nbio/taskpool 的 TaskPool 这种实现，然后本地实现并使用它。


### 003

Handler.OnClose、HandlerFuncs.Close都应该增加error参数让用户能够知道退出原因
drainPriorityInput读到的数据应该回调单独的OnPriorityData方法


### 004

c版本也应该回调单独的priority data相关的方法