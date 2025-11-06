package bybit_connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const staleDurationMs = int64(2_000)

type MessageHandler func(message []byte) error

func (b *WebSocket) handleIncomingMessages() {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
			_, message, err := b.conn.ReadMessage()
			if err != nil {
				fmt.Println("Error reading:", err)
				b.Disconnect()
				return
			}

			if b.handlePongMsg(message) {
				continue
			}

			if b.onMessage != nil {
				err := b.onMessage(message)
				if err != nil {
					fmt.Println("Error handling message:", err)
					return
				}
			}
		}
	}
}

func (b *WebSocket) SetMessageHandler(handler MessageHandler) {
	b.onMessage = handler
}

type opMsg struct {
	Op      string `json:"op"`
	RetMsg  string `json:"ret_msg"`
	Success bool   `json:"success"`
}

func (b *WebSocket) handlePongMsg(message []byte) bool {
	if !strings.Contains(string(message), `"pong"`) {
		return false
	}
	var msg opMsg
	err := json.Unmarshal(message, &msg)
	if err != nil {
		fmt.Println("Error unmarshal:", err)
		return false
	}
	switch msg.Op {
	case "pong":
		b.lastPongTime.Store(time.Now().UnixMilli())
		return true
	case "ping":
		if msg.RetMsg == "pong" && msg.Success == true {
			b.lastPongTime.Store(time.Now().UnixMilli())
			return true
		}
	}
	return false
}

type WebSocket struct {
	conn         *websocket.Conn
	url          string
	apiKey       string
	apiSecret    string
	maxAliveTime string
	pingInterval int
	onMessage    MessageHandler
	ctx          context.Context
	cancel       context.CancelFunc
	isConnected  *atomic.Bool
	lastPingTime *atomic.Int64
	lastPongTime *atomic.Int64

	writeMu sync.Mutex
}

type WebsocketOption func(*WebSocket)

func WithPingInterval(pingInterval int) WebsocketOption {
	return func(c *WebSocket) {
		c.pingInterval = pingInterval
	}
}

func WithMaxAliveTime(maxAliveTime string) WebsocketOption {
	return func(c *WebSocket) {
		c.maxAliveTime = maxAliveTime
	}
}

func NewBybitPrivateWebSocket(url, apiKey, apiSecret string, handler MessageHandler, options ...WebsocketOption) *WebSocket {
	c := &WebSocket{
		url:          url,
		apiKey:       apiKey,
		apiSecret:    apiSecret,
		maxAliveTime: "",
		pingInterval: 20,
		onMessage:    handler,
		lastPingTime: &atomic.Int64{},
		lastPongTime: &atomic.Int64{},
		isConnected:  &atomic.Bool{},
	}

	// Apply the provided options
	for _, opt := range options {
		opt(c)
	}

	return c
}

func NewBybitPublicWebSocket(url string, handler MessageHandler) *WebSocket {
	c := &WebSocket{
		url:          url,
		pingInterval: 20, // default is 20 seconds
		onMessage:    handler,
		lastPingTime: &atomic.Int64{},
		lastPongTime: &atomic.Int64{},
		isConnected:  &atomic.Bool{},
	}

	return c
}

func (b *WebSocket) Connect(ctx context.Context) error {
	// reset ping pong status
	b.lastPingTime.Store(0)
	b.lastPongTime.Store(0)

	wssUrl := b.url
	if b.maxAliveTime != "" {
		wssUrl += "?max_alive_time=" + b.maxAliveTime
	}
	conn, _, err := websocket.DefaultDialer.Dial(wssUrl, nil)
	if err != nil {
		return fmt.Errorf("websocket connect error: %w", err)
	}

	b.conn = conn

	if b.requiresAuthentication() {
		if err = b.sendAuth(); err != nil {
			return fmt.Errorf("failed to send auth: %w", err)
		}
	}
	b.isConnected.Store(true)

	//go b.monitorConnection()

	b.ctx, b.cancel = context.WithCancel(ctx)
	go b.handleIncomingMessages()
	go b.ping()

	return nil
}

func (b *WebSocket) SendSubscription(args []string) (*WebSocket, error) {
	reqID := uuid.New().String()
	subMessage := map[string]interface{}{
		"req_id": reqID,
		"op":     "subscribe",
		"args":   args,
	}
	fmt.Println("subscribe msg:", fmt.Sprintf("%v", subMessage["args"]))
	if err := b.sendAsJson(subMessage); err != nil {
		fmt.Println("Failed to send subscription:", err)
		return b, err
	}
	fmt.Println("Subscription sent successfully.")
	return b, nil
}

// SendRequest sendRequest sends a custom request over the WebSocket connection.
func (b *WebSocket) SendRequest(op string, args map[string]interface{}, headers map[string]string, reqId ...string) (*WebSocket, error) {
	finalReqId := uuid.New().String()
	if len(reqId) > 0 && reqId[0] != "" {
		finalReqId = reqId[0]
	}

	request := map[string]interface{}{
		"reqId":  finalReqId,
		"header": headers,
		"op":     op,
		"args":   []interface{}{args},
	}
	fmt.Println("request headers:", fmt.Sprintf("%v", request["header"]))
	fmt.Println("request op channel:", fmt.Sprintf("%v", request["op"]))
	fmt.Println("request msg:", fmt.Sprintf("%v", request["args"]))
	if err := b.sendAsJson(request); err != nil {
		fmt.Println("Failed to send websocket trade request:", err)
		return b, err
	}
	fmt.Println("Successfully sent websocket trade request.")
	return b, nil
}

func (b *WebSocket) SendTradeRequest(tradeTruest map[string]interface{}) (*WebSocket, error) {
	fmt.Println("trade request headers:", fmt.Sprintf("%v", tradeTruest["header"]))
	fmt.Println("trade request op channel:", fmt.Sprintf("%v", tradeTruest["op"]))
	fmt.Println("trade request msg:", fmt.Sprintf("%v", tradeTruest["args"]))
	if err := b.sendAsJson(tradeTruest); err != nil {
		fmt.Println("Failed to send websocket trade request:", err)
		return b, err
	}
	fmt.Println("Successfully sent websocket trade request.")
	return b, nil
}

func (b *WebSocket) ping() {
	if b.pingInterval <= 0 {
		fmt.Println("Ping interval is set to a non-positive value.")
		return
	}

	ticker := time.NewTicker(time.Duration(b.pingInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if b.lastPingTime.Load() > b.lastPongTime.Load()+staleDurationMs {
				fmt.Println("Stale connection")
				b.Disconnect()
				return
			}

			currentTime := time.Now().UnixMilli()
			pingMessage := map[string]string{
				"op":     "ping",
				"req_id": fmt.Sprintf("%d", currentTime),
			}

			if err := b.sendAsJson(pingMessage); err != nil {
				fmt.Println("Failed to send ping:", err)
				b.Disconnect()
				return
			}
			b.lastPingTime.Store(currentTime)
			fmt.Println("Ping sent with UTC time:", currentTime)

		case <-b.ctx.Done():
			fmt.Println("Ping context closed, stopping ping.")
			return
		}
	}
}

func (b *WebSocket) IsConnected() bool {
	return b.isConnected.Load()
}

func (b *WebSocket) Disconnect() error {
	b.cancel()
	b.isConnected.Store(false)
	return b.conn.Close()
}

func (b *WebSocket) requiresAuthentication() bool {
	return b.url == WEBSOCKET_PRIVATE_MAINNET || b.url == WEBSOCKET_PRIVATE_TESTNET ||
		b.url == WEBSOCKET_TRADE_MAINNET || b.url == WEBSOCKET_TRADE_TESTNET ||
		b.url == WEBSOCKET_TRADE_DEMO || b.url == WEBSOCKET_PRIVATE_DEMO
	// v3 offline
	/*
		b.url == V3_CONTRACT_PRIVATE ||
			b.url == V3_UNIFIED_PRIVATE ||
			b.url == V3_SPOT_PRIVATE
	*/
}

func (b *WebSocket) sendAuth() error {
	// Get current Unix time in milliseconds
	expires := time.Now().UnixNano()/1e6 + 10000
	val := fmt.Sprintf("GET/realtime%d", expires)

	h := hmac.New(sha256.New, []byte(b.apiSecret))
	h.Write([]byte(val))

	// Convert to hexadecimal instead of base64
	signature := hex.EncodeToString(h.Sum(nil))
	fmt.Println("signature generated : " + signature)

	authMessage := map[string]interface{}{
		"req_id": uuid.New(),
		"op":     "auth",
		"args":   []interface{}{b.apiKey, expires, signature},
	}
	fmt.Println("auth args:", fmt.Sprintf("%v", authMessage["args"]))
	return b.sendAsJson(authMessage)
}

func (b *WebSocket) sendAsJson(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.send(string(data))
}

func (b *WebSocket) send(message string) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return b.conn.WriteMessage(websocket.TextMessage, []byte(message))
}
