package chainpool_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/alimovahedi/chainpool"
)

func ExampleNew() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv.Close()

	m, err := chainpool.New(chainpool.Config{
		Defaults: chainpool.Defaults{RateRPS: 50, Timeout: chainpool.Duration(5 * time.Second)},
		Chains: map[string]chainpool.ChainConfig{
			"ethereum": {Nodes: []chainpool.NodeConfig{
				{Name: "local", BaseURL: srv.URL, Priority: 100, RateRPS: 50},
			}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer m.Close()

	resp, err := m.Do(context.Background(), "ethereum", chainpool.Request{Method: http.MethodGet})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode)
	// Output: 200
}
