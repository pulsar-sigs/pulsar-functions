package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/allegro/bigcache"
	"github.com/apache/pulsar/pulsar-function-go/logutil"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func inactiveAlertHandler(ctx context.Context, in []byte) error {
	if fc, ok := pf.FromContext(ctx); ok {
		topic := fc.GetCurrentRecord().Topic()
		activeCache.Set(topic, []byte("1"))
		// logutil.Info("function ID is:%s, ", fc.GetFuncID())
		// logutil.Info("function version is:%s\n", fc.GetFuncVersion())
	}
	return nil
}

func sendDingMsg(msg string) {
	webHook := `https://oapi.dingtalk.com/robot/send?access_token=` + token
	content := `{"msgtype": "text",
	"text": {"content": "` + msg + `"}
	}`
	req, err := http.NewRequest("POST", webHook, strings.NewReader(content))
	if err != nil {
		logutil.Error("post.dingtalk.webhook.before.failed!", err)
		// handle error
	}

	client := &http.Client{}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		logutil.Error("post.dingtalk.webhook.failed!", err)
		// handle error
	}
}

var activeCache *bigcache.BigCache

//dingtalk webhook token
var token string

func init() {

	if dingTalkToken, ok := pf.GetUserConfMap()["dingTalkToken"]; !ok {
		logutil.Fatal("have not dingTalkToken config! please set userConfig for dingTalkToken")
	} else {
		token = dingTalkToken.(string)
	}

	config := bigcache.Config{
		Shards:     1024,
		LifeWindow: 1 * time.Minute,
		OnRemoveWithReason: func(key string, entry []byte, reason bigcache.RemoveReason) {
			logutil.Infof("this.key:%s.is.remove,%d", key, reason)
			sendDingMsg("this.topic:" + key + ".is.inactive!")
		},
	}
	activeCache, _ = bigcache.NewBigCache(config)
}

func main() {
	pf.Start(inactiveAlertHandler)
}
