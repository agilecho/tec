##1.第三方库
原库已集成至代码库，无须重新安装，部分代码已精简  
更新至2020-11-01  
github.com/streadway/amqp.git  
github.com/beanstalkd/go-beanstalk.git  
github.com/go-mgo/mgo.git 
github.com/go-sql-driver/mysql.git   
github.com/gomodule/redigo.git  
github.com/gorilla/websocket.git  

##2、安装
go get github.com/agilecho/tec

##3、项目
创建项目文件夹，例如demo，目录结构如下：
<pre>
demo
    config
        dev.ini
    src
        源代码位置
    demo.go
</pre>

dev.ini 全部参数示例：
<pre>
[app]
name = demo
host = 0.0.0.0
port = 9500
token = token@2020
memory = 1024

[ws]
host = 0.0.0.0
port = 1234
path = /echo
token = token@2019
origin = *
version = 1

[redis]
host = 127.0.0.1
port = 6379
passwd =
prefix =
pool = 5
active = 1
logs = ROOT_PATH/logs/redis

[cookie]
domain = demo.com
path = /
expire =
secure = false
httponly =
prefix =

[session]
type = file
path = ROOT_PATH/tmp
expire = 86400
prefix =

[template]
path = ROOT_PATH/tpl
extension = .html
define =

[mysql]
host = 127.0.0.1
port = 3306
user = root
passwd =
database = demo
charset = utf8mb4
deploy =
pool = 5
active = 1
timeout = 120
debug = true
logs = ROOT_PATH/logs/mysql

[mongo]
host = 127.0.0.1
port = 27017
user = root
passwd =
database = admin
pool = 5
logs = ROOT_PATH/logs/mongo

[mq]
type = rabbit
host = 127.0.0.1
port = 5672
user = guest
passwd = guest
vhost = /
exchange = exchange
logs = ROOT_PATH/logs/mq
</pre>

项目中不使用，则删除节点  

demo.go示例如下：  
*WEB方式*  
<pre>
package main

import (
    "github.com/agilecho/tec"
)

func main() {
    app := tec.New()

    app.Router.Add("", func(ctx *tec.Context) {
        ctx.Text("hello world.")
    })

    app.Run()
}
</pre>
打开浏览器访问http://127.0.0.1:9500即可  

*WebSocket方式*
<pre>
package main

import (
    "github.com/agilecho/tec"
    "github.com/agilecho/tec/ws"
    "fmt"
)

func main() {
    app := tec.New()

    app.Bind("push", func(req *ws.Request) {
        fmt.Println("ws push")
    })

    app.Bind("connect", func(req *ws.Request) {
        fmt.Println("ws connect")
    })

    app.Bind("message", func (req *ws.Request, message string) {
        fmt.Println("ws message")
    })

    app.Bind("close", func(req *ws.Request) {
        fmt.Println("ws close")
    })

    app.RunWS()
}
</pre>

*CLI方式*
<pre>
package main

import (
    "fmt"
    "github.com/agilecho/tec"
)

func main() {
    tec.Cli(func() {
        fmt.Println("hello world")
    })

    // 如果需要监听
    tec.Notify()
}
</pre>

##4、部署
1.生成可执行文件go build demo.go  
2.将demo和资源文件夹复制到Linux服务器  
3.进入服务器目录中执行  
<code>
./demo / prod > demo.log &
</code>