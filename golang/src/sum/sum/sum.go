package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchanges map[string]middleware.Middleware
	exchangeSums    middleware.Middleware
	config          SumConfig
	mu              sync.Mutex
	cond            *sync.Cond
	clientFruits    map[int64]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	exchangeSums, err := middleware.CreateExchangeMiddleware(config.SumPrefix, []string{"broadcast"}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	fail := false
	outputExchanges := make(map[string]middleware.Middleware)
	for i := range config.AggregationAmount {
		key := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputExchanges[key], err = middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{key}, connSettings)
		if err != nil {
			fail = true
			break
		}
	}
	if fail {
		for _, exchange := range outputExchanges {
			exchange.Close()
		}
		inputQueue.Close()
		exchangeSums.Close()
		return nil, err
	}

	sum := &Sum{
		inputQueue:      inputQueue,
		outputExchanges: outputExchanges,
		exchangeSums:    exchangeSums,
		config:          config,
		clientFruits:    map[int64]map[string]fruititem.FruitItem{},
	}
	sum.cond = sync.NewCond(&sum.mu)
	return sum, nil
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	slog.Info("Stopping consuming...")
	sum.inputQueue.StopConsuming()
	sum.exchangeSums.StopConsuming()
	slog.Info("Closing middlewares...")
	for _, exchange := range sum.outputExchanges {
		exchange.Close()
	}
	sum.inputQueue.Close()
	sum.exchangeSums.Close()
}

func (sum *Sum) Run() {
	go sum.handleSignals()

	go sum.exchangeSums.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessageExchange(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessageExchange(msg middleware.Message, ack, nack func()) {
	controlMessage, err := inner.DeserializeControlMessage(&msg)
	if err != nil {
		// Nack?
		slog.Error("While deserializing message from exchange sums")
		nack()
		return
	}
	slog.Info("Receive EOF message from exchange sums", "controlMessage", controlMessage)
	sum.mu.Lock()
	// Entiendo que en este punto devuelvo una referencia de un elemento compartido,
	// pero como inmediatamente hago un delete, lo saco del mapa, quedandome solo yo con la referencia y sacandola
	// del mapa que usan otros hilos
	fruits, ok := sum.clientFruits[controlMessage.ClientID]
	delete(sum.clientFruits, controlMessage.ClientID)
	sum.mu.Unlock()

	err = sum.processEOF(controlMessage.ClientID, fruits, ok)
	if err != nil {
		// nack?
		nack()
		return
	}
	err = sum.processFF(controlMessage.ClientID)
	if err != nil {
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer sum.mu.Unlock()
	sum.mu.Lock()

	fruitRecords, clientID, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
			nack()
			return
		}
		ack()
		return
	}
	if err := sum.handleDataMessage(fruitRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	controlEOFMessage := inner.ControlMessage{Type: inner.TypeEOF, ClientID: clientID}
	err := sum.sendMessageToExchangeSums(controlEOFMessage)
	if err != nil {
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID int64) error {
	if _, ok := sum.clientFruits[clientID]; !ok {
		slog.Info("Client new", "clientID", clientID)
		sum.clientFruits[clientID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.clientFruits[clientID][fruitRecord.Fruit]
		if ok {
			sum.clientFruits[clientID][fruitRecord.Fruit] = sum.clientFruits[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.clientFruits[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) sendFruitsToOutput(clientID int64, fruitsItemMap map[string]fruititem.FruitItem) error {
	for key := range fruitsItemMap {
		fruitRecord := []fruititem.FruitItem{fruitsItemMap[key]}
		if len(fruitRecord) == 0 {
			continue
		}
		keyExchange := sum.getKeyForExchange(clientID, fruitRecord[0].Fruit)
		err := sum.sendToOutputExchange(keyExchange, fruitRecord, clientID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sum *Sum) sendMessageToExchangeSums(controlMessage inner.ControlMessage) error {
	message, err := inner.SerializeControlMessage(controlMessage)
	if err != nil {
		slog.Debug("While serializing message to other sums", "err", err, "controlMessage", controlMessage)
		return err
	}
	if err := sum.exchangeSums.Send(*message); err != nil {
		slog.Debug("While sending message to other sums", "err", err, "controlMessage", controlMessage)
		return err
	}
	return nil
}

func (sum *Sum) processFF(clientID int64) error {
	for key, _ := range sum.outputExchanges {
		if err := sum.sendToOutputExchange(key, []fruititem.FruitItem{}, clientID); err != nil {
			slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
			return err
		}
	}
	slog.Info("Sent EOF to aggs", "clientID", clientID)
	return nil
}

func (sum *Sum) processEOF(clientID int64, fruits map[string]fruititem.FruitItem, ok bool) error {
	if ok {
		slog.Info("fruits sent", "fruits", fruits, "clientID", clientID)
		err := sum.sendFruitsToOutput(clientID, fruits)
		if err != nil {
			// Tengo que nack?
			return err
		}
	} else {
		slog.Info("Dont send fruits")
	}
	return nil
}

func (sum *Sum) getKeyForExchange(clientID int64, fruitName string) string {
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%d-%s", clientID, fruitName)))
	idx := int(hash.Sum32()) % sum.config.AggregationAmount
	return fmt.Sprintf("%s_%d", sum.config.AggregationPrefix, idx)
}

func (sum *Sum) sendToOutputExchange(key string, messageFruit []fruititem.FruitItem, clientID int64) error {
	message, err := inner.SerializeMessage(messageFruit, clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	if err := sum.outputExchanges[key].Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
