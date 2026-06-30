package gateway_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/amovah/chainpool"
	"github.com/amovah/chainpool/gateway"
)

func ExampleServe() {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer upstream.Close()

	m, err := chainpool.New(chainpool.Config{
		Defaults: chainpool.Defaults{RateRPS: 50, Timeout: chainpool.Duration(5 * time.Second)},
		Chains: map[string]chainpool.ChainConfig{
			"ethereum": {Nodes: []chainpool.NodeConfig{
				{Name: "local", BaseURL: upstream.URL, Priority: 100, RateRPS: 50},
			}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In real use, run Serve in its own goroutine and dial it from any client:
	//   ethclient.Dial("http://localhost:8545/ethereum")
	go func() { _ = gateway.Serve(ctx, m, "127.0.0.1:8545") }()

	fmt.Println("gateway serving /ethereum on 127.0.0.1:8545")
	// Output: gateway serving /ethereum on 127.0.0.1:8545
}
