使用框架前必将目录定位至GOPATH的src后，按以下步骤操作。

0.第三方库
  更新至2020-01-01
  github.com/go-sql-driver/mysql.git
  github.com/gomodule/redigo.git

  更新至2020-10-01
  github.com/go-mgo/mgo.git
  github.com/streadway/amqp.git
  github.com/beanstalkd/go-beanstalk.git
  github.com/gorilla/websocket.git

1、安装
git clone -b golang https://gitee.com/agilecho/tec.git tec

2、项目
创建项目文件夹，例如demo，目录结构如下：
src
    demo
        app
            config
                dev.ini
        demo.go

配置文件示例如下：
    [app]
    name = demo
    host = 127.0.0.1
    port = 9500
    token = token@2019
    memory = 1024

    [ws]
    host = 127.0.0.1
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


启动脚本demo.go示例代码如下：
2.1、WEB方式
    package main

    import (
        "tec"
    )

    func main() {
        app := tec.New()

        app.Router.Add("", func(ctx *tec.Context) {
            ctx.Text("hello world.")
        })

        app.Run()
    }

    打开浏览器访问http://127.0.0.1:9500即可
2.2、WebSocket方式
    package main

    import (
    	"tec"
    	"tec/ws"
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

2.3、CLI方式
    package main

    import (
    	"fmt"
        "tec"
    )

    func main() {
        tec.Cli(func() {
            fmt.Println("hello world")
        })

        // 如果需要监听
        tec.Notify()
    }

3、GoLand配置
将项目定位至GOAPTH目录，按1-2步骤创建项目，运行时需添加参数，方法如下：
3.1、选择GoBuild中相应的启动脚本，在右方的Working directory为脚本全目录
3.2、直接运行启动脚本

如需指定运行时配置文件，如prod.ini，在参数后继续添加参数prod，以空格隔开

4、部署
4.1、生成可执行文件go build demo.go
4.2、将demo和app中资源文件复制到Linux服务器项目文件夹中
4.3、进入项目文件夹中执行
    ./demo / prod > demo.log &