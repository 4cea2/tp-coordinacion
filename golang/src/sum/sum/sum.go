package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

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
	processing      bool
	keys            []string
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
	keys := []string{}
	for i := range config.AggregationAmount {
		key := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		keys = append(keys, key)
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
		processing:      false,
		keys:            keys,
		clientFruits:    map[int64]map[string]fruititem.FruitItem{},
	}
	sum.cond = sync.NewCond(&sum.mu)
	return sum, nil
}

func (sum *Sum) Run() {
	go sum.exchangeSums.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessageExchange(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessageExchange(msg middleware.Message, ack, nack func()) {
	defer ack()
	controlMessage, err := inner.DeserializeControlMessage(&msg)
	if err != nil {
		// Nack?
		slog.Error("While deserializing message from exchange sums")
		return
	}
	slog.Info("Receive message from exchange sums", "controlMessage", controlMessage)
	if controlMessage.Type == inner.TypeEOF {
		sum.mu.Lock()
		for sum.processing {
			sum.cond.Wait()
		}
		fruits, ok := sum.clientFruits[controlMessage.ClientID]
		delete(sum.clientFruits, controlMessage.ClientID)
		sum.mu.Unlock()

		err := sum.processEOF(*controlMessage, fruits, ok)
		if err != nil {
			// nack?
			return
		}
	} else if controlMessage.Type == inner.TypeAckEOF {
		if controlMessage.OriginID != sum.config.Id {
			// Si no fui el que envio el EOF, no me interesa
			return
		}
		err := sum.processFF(controlMessage.ClientID)
		if err != nil {
			// NACK?
			return
		}
	} else {
		// *c preocupa
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	defer func() {
		sum.mu.Lock()
		sum.processing = false
		sum.cond.Broadcast()
		sum.mu.Unlock()
	}()
	sum.mu.Lock()
	sum.processing = true
	sum.mu.Unlock()

	fruitRecords, clientID, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, "clientID", clientID)
		}
		return
	}
	if err := sum.handleDataMessage(fruitRecords, clientID); err != nil {
		slog.Error("While handling data message", "err", err, "clientID", clientID)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	controlEOFMessage := inner.ControlMessage{Type: inner.TypeEOF, ClientID: clientID, OriginID: sum.config.Id}
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
		err := sum.sendToOutputExchanges(clientID, fruitRecord)
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
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage, clientID)

	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	for _, key := range sum.keys {
		if err := sum.outputExchanges[key].Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
			return err
		}
	}
	slog.Info("Sent EOF to aggs", "clientID", clientID)
	return nil
}

func (sum *Sum) processEOF(controlMessage inner.ControlMessage, fruits map[string]fruititem.FruitItem, ok bool) error {
	if ok {
		slog.Info("fruits sent", "fruits", fruits, "controlMessage", controlMessage)
		err := sum.sendFruitsToOutput(controlMessage.ClientID, fruits)
		if err != nil {
			// Tengo que nack?
			return err
		}
	} else {
		slog.Info("Dont send fruits")
	}

	controlAckEOFMessage := inner.ControlMessage{
		Type:      inner.TypeAckEOF,
		ClientID:  controlMessage.ClientID,
		OriginID:  sum.config.Id,
		ReplyToID: controlMessage.OriginID,
	}
	err := sum.sendMessageToExchangeSums(controlAckEOFMessage)
	if err != nil {
		// nack?
		return err
	}
	return nil
}

func (sum *Sum) getKeyForExchange(clientID int64, fruitName string) string {
	hash := fnv.New32a()

	hash.Write([]byte(fmt.Sprintf("%d", clientID)))
	hash.Write([]byte(fruitName))

	idx := int(hash.Sum32()) % sum.config.AggregationAmount

	return fmt.Sprintf("%s_%d", sum.config.AggregationPrefix, idx)
}

func (sum *Sum) sendToOutputExchanges(clientID int64, fruitMessage []fruititem.FruitItem) error {
	message, err := inner.SerializeMessage(fruitMessage, clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
		return err
	}
	fruitName := fmt.Sprintf("%d", clientID) // No envio por fruta cuando me llega el EOF, sino por clientID
	if len(fruitMessage) == 1 {
		fruitName = fruitMessage[0].Fruit
	}
	keyExchange := sum.getKeyForExchange(clientID, fruitName)
	if err := sum.outputExchanges[keyExchange].Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}
