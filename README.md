##1.第三方库
原库已集成至代码库，无须重新安装，部分代码已精简  
更新至2021-06-01  
github.com/robfig/cron.git  
github.com/streadway/amqp.git  
github.com/go-mgo/mgo.git  
github.com/go-sql-driver/mysql.git   
github.com/gomodule/redigo.git  
github.com/gorilla/websocket.git  
github.com/stianeikeland/go-rpio.git

##2、安装
go get -u github.com/agilecho/tec

##3、项目
创建项目文件夹，例如demo，目录结构如下：
<pre>
demo
    config
        dev.ini
    src
        源代码文件
    static
        静态资源文件
    tpl
        模板文件
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
logs = LOG_PATH/redis

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
logs = LOG_PATH/mysql

[mongo]
host = 127.0.0.1
port = 27017
user = root
passwd =
database = admin
pool = 5
logs = LOG_PATH/mongo

[mq]
host = 127.0.0.1
port = 5672
user = guest
passwd = guest
vhost = /
exchange = exchange
logs = LOG_PATH/mq

[cron]
test = */5 * * * * ?

</pre>

项目中不使用，则删除节点  
 
*WEB方式*  
<pre>
package main

import (
    "github.com/agilecho/tec"
    "github.com/agilecho/tec/cron"
)

func main() {
    app := tec.New()
    
    // 添加默认路由
    app.Router.Add("", func(ctx *tec.Context) {
        ctx.Text("hello world.")
    })
    // 应用启动时执行
    app.Start(func(app *tec.App) {
        cron.Add(app.Config.Cron.Schedules["test"], func() {
            fmt.Println(tec.Microtime())
        }).Start()
    })
    // 没有发现路由时执行
    app.Empty(func(ctx *tec.Context){
        ctx.Text("not find")
    })
    // 路由执行前过滤器
    app.Before(func(ctx *tec.Context) bool {
        fmt.Println("handler filter")
        return true
    })
    // 路由执行后过滤器
    app.After(func(ctx *tec.Context, method string, data interface{}) {
        fmt.Println("context response")
    })
    
    app.Run()
}

// 路由
app.Router.Add("地址", Handler)
地址规则:/模块/控制器/方法
空 解析结果 /home/index/index
/a/b/c 解析结果 /a/b/c
/a/b 解析结果 /a/b/index
/a 解析结果 /a/index/index

不支持地址变量
</pre>
打开浏览器访问http://localhost:9500  

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
    
    // 广播
    app.Bind("push", func(req *ws.Request) {
        fmt.Println("ws push")
    })
    
    // 连接
    app.Bind("connect", func(req *ws.Request) {
        fmt.Println("ws connect")
    })
    
    // 发送消息
    app.Bind("message", func (req *ws.Request, message string) {
        fmt.Println("ws message")
    })
    
    // 断开
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

##4、工具类  
###4.1.数据库  
默认MySQL，支持主从库
<pre>
// 添加记录
insertid := db.Insert("表名", db.Row{"字段名":"字段值", ...参数})
// 更新记录
affected := db.Update("表名", "SQL语句", db.Row{"字段名":"字段值", ...}, "WHERE条件", ...参数)
// 删除记录
affected := db.Delete("表名", "WHERE条件", ...参数)
// 获取单条记录
row := db.FetchFirst("SQL语句", ...参数)
// 获取多条记录
rows := db.FetchRows("SQL语句", ...参数)

// 事务
tx := db.Trans()
tx.Insert(...)
tx.Update(...)
err := tx.Commit()
if err != nil {
    tx.Rollback()
}

// 表操作
db.Table("表名").Insert(db.Row{})
db.Table("表名").Where("id", "=", 1).Update(db.Row{})
db.Table("表名").Where("id", "=", 1).Delete()
db.Table("表名").Where("id", "=", 1).First()
db.Table("表名").Alias("a").Join("xxx", "").Where("id", ">", 1).Rows()

// 结构体
user := User{name:""}
users := []User{}

db.Table("表名").Save(&user)
db.Table("表名").Where().Find(&user)
db.Table("表名").Where().Search(&users)

// 自定义连接
database := db.New(&db.Config{
    Host: "127.0.0.1",
    Port: "3306",
    User: "root",
    Passwd: "mysql",
    Database: "test",
    Charset: "utf8mb4",
    Logs: tec.LOG_PATH + "/mysql",
})

// 手动关闭
database.Close()
</pre>

###4.2.缓存  
默认使用Redis
<pre>
cache.常用方法,如Get、Set、SetNx、Incr

// 自定义连接
redis := cache.New(&cache.Config{
    Host: "127.0.0.1",
    Port: "6379",
    Prefix: "",
    Logs: tec.LOG_PATH + "/redis",
})

// 手动关闭
redis.Close()
</pre>

###4.3.Mongodb 
<pre>
// 静态方法
mongo.ListDBs()
mongo.ListCollections("库名")
mongo.CreateCollection("集合名")
mongo.DropCollection("集合名")
mongo.SelectCollection("库名", "集合名")

// 集合方法
Insert()
Update()
Remove()
One()
Find()

// 自定义连接
mongodb := mongo.New(&mongo.Config{
    Host: "127.0.0.1",
    Port: "27017",
    User: "root",
    Passwd: "",
    Database: "admin",
    Logs: tec.LOG_PATH + "/mongo",
})

// 手动关闭
mongodb.Close()
</pre>

###4.4.消息队列  
默认使用RabbitMQ
<pre>
// 发送
mq.DirectQueue("demo").Put("hello world.")

// 消费
go mq.DirectQueue("demo").Reserve(func(queue *mq.Queue, message *mq.Message) {
    fmt.Println(message.Body)
    queue.Delete(message)
})

// 自定义连接
rabbitMq := mq.New(&mq.Config{
    Host: "127.0.0.1",
    Port: "5672",
    User: "guest",
    Passwd:  "guest",
    VHost: "/",
    Exchange: "exchange",
    Logs: tec.LOG_PATH + "/mq",
})

// 手动关闭
rabbitMq.Close()
</pre>

###4.5.图片 
<pre>
// 验证码
captcha := image.NewCaptcha(tec.Random(), 100, 10)
captcha.PNG()

// 图片处理
img := image.New("图片路径")
img.Circle(image.Option{Width:100, Height:100, image.Center, Target:"保存路径"})
img.Merge("图片路径", image.Option{})
img.Text("图片路径", image.Option{})
img.Thumb("图片路径", image.Option{})

img.Save()
</pre>

###4.6.定时任务 
<pre>
// 定时任务
cron.Add("cron表达式", func() {
    fmt.Println("hello world.")
}).Start()
</pre>

###4.7.Rpio 
<pre>
err := rpio.Open()

pin := rpio.Pin(10)

pin.Output()
pin.High()
pin.Low()
pin.Toggle()

pin.Input()
res := pin.Read()

pin.Mode(rpio.Output)
pin.Write(rpio.High)
pin.PullUp()
pin.PullDown()
pin.PullOff()

pin.Pull(rpio.PullUp)

rpio.Close()
</pre>

##5、部署  
1.编译 go build demo.go  
2.打包 ./demo -zip  
2.将demo.zip部署到服务器  
3.进入服务器目录中执行  
<pre>
unzip demo.zip -d 指定目录

使用本机hostname配置文件ini
./demo >demo.log 2>&1 &

使用prod配置文件ini
./demo prod >demo.log 2>&1 &
</pre>