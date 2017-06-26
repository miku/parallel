// Request links in parallel.
//
//     $ go run examples/fetchall.go
//     {"elapsed":"0.18","size":116946,"url":"http://www.jd.com"}
//     {"elapsed":"0.10","size":10401,"url":"http://www.google.de"}
//     {"elapsed":"0.29","size":10588,"url":"http://www.google.co.jp"}
//     {"elapsed":"0.29","size":13180,"url":"http://www.google.co.in"}
//     {"elapsed":"0.36","size":10431,"url":"http://www.google.com"}
//     {"elapsed":"0.10","size":10885,"url":"http://www.google.com.br"}
//     {"elapsed":"0.12","size":10410,"url":"http://www.google.co.uk"}
//     {"elapsed":"0.47","size":249915,"url":"http://www.qq.com"}
//     {"elapsed":"0.54","size":86364,"url":"http://www.wikipedia.org"}
//     {"elapsed":"0.19","size":10513,"url":"http://www.google.fr"}
//     {"elapsed":"0.18","size":10961,"url":"http://www.google.ru"}
//     ...

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/miku/parallel"
)

// unzip -p top-1m.csv.zip | head -30 | awk -F, '{"http:www.//"$2}'
var input = `
http://www.google.com
http://www.youtube.com
http://www.baidu.com
http://www.wikipedia.org
http://www.yahoo.com
http://www.google.co.in
http://www.reddit.com
http://www.qq.com
http://www.taobao.com
http://www.amazon.com
http://www.twitter.com
http://www.google.co.jp
http://www.tmall.com
http://www.live.com
http://www.vk.com
http://www.sohu.com
http://www.jd.com
http://www.sina.com.cn
http://www.weibo.com
http://www.360.cn
http://www.google.de
http://www.google.co.uk
http://www.linkedin.com
http://www.google.com.br
http://www.google.fr
http://www.google.ru
http://www.yandex.ru
http://www.google.com.hk
http://www.netflix.com
http://www.yahoo.co.jp
http://www.google.it
http://www.t.co
http://www.ebay.com
http://www.imgur.com
http://www.google.es
http://www.pornhub.com
http://www.msn.com
http://www.bing.com
http://www.google.com.mx
http://www.google.ca
http://www.twitch.tv
http://www.tumblr.com
http://www.alipay.com
http://www.mail.ru
http://www.hao123.com
http://www.microsoft.com
http://www.aliexpress.com
http://www.wordpress.com
http://www.ok.ru
http://www.stackoverflow.com
http://www.imdb.com
http://www.github.com
http://www.blogspot.com
http://www.amazon.co.jp
http://www.pinterest.com
http://www.apple.com
http://www.office.com
http://www.google.com.tr
http://www.youth.cn
http://www.csdn.net
http://www.gmw.cn
http://www.wikia.com
http://www.popads.net
http://www.microsoftonline.com
http://www.google.com.au
http://www.google.com.tw
http://www.paypal.com
http://www.google.pl
http://www.diply.com
http://www.google.co.id
http://www.adobe.com
http://www.bongacams.com
http://www.coccoc.com
http://www.dropbox.com
http://www.googleusercontent.com
http://www.bbc.co.uk
http://www.soso.com
http://www.craigslist.org
http://www.amazon.de
http://www.google.co.th
http://www.pixnet.net
http://www.google.com.pk
http://www.google.com.ar
http://www.thepiratebay.org
http://www.amazon.in
http://www.google.com.eg
http://www.bbc.com
http://www.cnn.com
http://www.google.com.sa
http://www.tianya.cn
http://www.ub.uni-leipzig.de
http://www.uni-leipzig.de
`

// MarshalEnd marshals a value and appends a the given bytes at the end.
func MarshalEnd(v interface{}, end []byte) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return b, err
	}
	b = append(b, end...)
	return b, err
}

func main() {
	p := parallel.NewProcessor(strings.NewReader(input), os.Stdout, func(b []byte) ([]byte, error) {
		// Ignore empty lines.
		link := string(bytes.TrimSpace(b))
		if len(link) == 0 {
			return nil, nil
		}
		start := time.Now()
		resp, err := http.Get(link)
		if err != nil {
			log.Printf("HTTP failed, skipping %s: %s", link, err)
			return nil, nil
		}
		defer resp.Body.Close()
		n, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			log.Printf("HTTP read failed, skipping %s: %s", link, err)
			return nil, nil
		}
		elapsed := time.Since(start)
		return MarshalEnd(map[string]interface{}{
			"url":     link,
			"size":    n,
			"elapsed": fmt.Sprintf("%0.2f", elapsed.Seconds()),
		}, []byte("\n"))
	})
	p.BatchSize = 1
	p.NumWorkers = 20
	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
