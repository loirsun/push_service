package main

// 用Golang重写redis subscriber程序，将channel中拿到的message用http request发送给php推送api
// 替换了php常驻进程（开发人员对php GC不了解，所以换Golang实现）
// go程序设置了最大并发goroutine数量，保证php server api不会被推崩溃
//

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/pelletier/go-toml"
	"soulsense.cn/push_service/lg"
	"soulsense.cn/push_service/util"
)

var (
	configPath string
)

type Response struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

//
type Consumer struct {
	ps          *PushService
    // redis channel name which this consumer is binding to
	channel     string
    // A buffered channel plays the semaphore role
	sem         chan bool
    // Consumer just fetch message from redis channel and send a http post request to this api endpoint
	apiEndpoint string
}

type PushService struct {
	opts *Options

	redisClient *redis.Client
	httpClient  *http.Client
	consumers   []*Consumer
	waitGroup   util.WaitGroupWrapper
}

type Options struct {
	tree      *toml.Tree
	LogLevel  string
	LogPrefix string
	Logger    Logger
	logLevel  lg.LogLevel // private, not really an option

	// concurrency         int64
	redisAddress  string
	redisPassword string

	maxIdleConns        int64
	maxIdleConnsPerHost int64

	channels []map[string]interface{}
}

func NewPushService(opts *Options) *PushService {
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	p := &PushService{
		opts:      opts,
		consumers: make([]*Consumer, 0),
	}

	var err error
	opts.logLevel, err = lg.ParseLogLevel(opts.LogLevel, false)
	if err != nil {
		p.logf(LOG_FATAL, "%s", err)
		os.Exit(1)
	}

	r := redis.NewClient(&redis.Options{
		Addr:     opts.redisAddress,
		Password: opts.redisPassword,
		DB:       0,
	})
	pong, err := r.Ping().Result()

	if err != nil {
		p.logf(LOG_FATAL, "Failed to connect redis - %s", pong)
		os.Exit(1)
	}
	p.redisClient = r

	// see http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing/
	var customTransport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          int(opts.maxIdleConns),
		MaxIdleConnsPerHost:   int(opts.maxIdleConnsPerHost),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	p.httpClient = &http.Client{Transport: customTransport}
	return p
}

func (p *PushService) NewConsumer(channel string, concurrency int64, apiEndpoint string) *Consumer {
	consumer := &Consumer{
		channel:     channel,
		sem:         make(chan bool, concurrency),
		apiEndpoint: apiEndpoint,
	}
	consumer.ps = p
	return consumer
}

func (p *PushService) AddConsumer(consumer *Consumer) {
	p.consumers = append(p.consumers, consumer)
}

func (c *Consumer) SubscribeAndConsume() {
	pubsub := c.ps.redisClient.Subscribe(c.channel)
	c.ps.logf(LOG_INFO, "[%s] Subscribe channel success", c.channel)
	// Go channel which receives messages.
	ch := pubsub.Channel()
	c.ps.logf(LOG_INFO, "[%s] Start receiving message", c.channel)
	// Consume messages.
	for msg := range ch {
		// fmt.Println(msg.Channel, msg.Payload)
		c.sem <- true
		go c.CallApi(msg.Payload)
	}
}

func (p *PushService) Run(opts *Options) {
	for _, c := range p.consumers {
		cc := c
		p.waitGroup.Wrap(cc.SubscribeAndConsume)
	}
	p.waitGroup.Wait()
}

func (c *Consumer) CallApi(data string) {
	c.ps.logf(LOG_DEBUG, data)

	defer func() { <-c.sem }()

	resp, err := c.ps.httpClient.PostForm(c.apiEndpoint, url.Values{"message": {data}})
	if err != nil {
		c.ps.logf(LOG_ERROR, "[%s] http post failed - %s", c.channel, err)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.ps.logf(LOG_ERROR, "[%s] read response body failed - %s", c.channel, data)
		resp.Body.Close()
		return
	}
	response := &Response{}
	err = json.Unmarshal(body, response)
	if err != nil {
		c.ps.logf(LOG_ERROR, "[%s] parse json response failed - %s %x", c.channel, err, body)
		goto exit
	}
	if response.Code != 0 {
		c.ps.logf(LOG_ERROR, "[%s] json response tells error - %s", c.channel, response.Message)
		goto exit
	}
exit:
	resp.Body.Close()
}

func main() {
	flag.StringVar(&configPath, "c", "./config.toml", "path to configure file")

	flag.Parse()
	opts, err := loadConfig(configPath)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Load config successfully.")
	}

	pushService := NewPushService(opts)
	for _, c := range opts.channels {
		pushService.AddConsumer(pushService.NewConsumer(c["name"].(string), c["concurrency"].(int64), c["api_endpoint"].(string)))
	}
	pushService.Run(opts)
}

func loadConfig(path string) (opts *Options, err error) {
	conf, err := toml.LoadFile(path)
	if err != nil {
		return nil, errors.New("Loading config file failed!")
	}
	opts = &Options{
		tree:      conf,
		LogPrefix: "[push] ",
		LogLevel:  "info",
		channels:  make([]map[string]interface{}, 0),
	}
	opts.LogPrefix = conf.Get("logger.log_prefix").(string)
	opts.LogLevel = conf.Get("logger.log_level").(string)

	opts.redisAddress = conf.Get("database.redis_addr").(string)
	opts.redisPassword = conf.Get("database.redis_password").(string)
	opts.maxIdleConns = conf.Get("http_client.max_idle_conns").(int64)
	opts.maxIdleConnsPerHost = conf.Get("http_client.max_idle_conns_per_host").(int64)

	tomlMap := opts.tree.ToMap()
	channels, exist := tomlMap["channels"].([]interface{})
	if !exist {
		panic(errors.New("Config [[channels]] missing!"))
	}

	for _, c := range channels {
		opts.channels = append(opts.channels, c.(map[string]interface{}))
	}

	return opts, nil
}
