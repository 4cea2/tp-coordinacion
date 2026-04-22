package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	clientFruits  map[int64]map[string]fruititem.FruitItem
	counterEOFs   map[int64]int
	config        AggregationConfig
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	agg := &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		clientFruits:  map[int64]map[string]fruititem.FruitItem{},
		counterEOFs:   map[int64]int{},
		config:        config,
	}
	return agg, nil
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	slog.Info("Stopping consuming...")
	aggregation.inputExchange.StopConsuming()
	slog.Info("Closing middlewares...")
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()

	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	fruitRecords, clientID, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		nack()
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, clientID)
			nack()
			return
		}
		ack()
		return
	}
	aggregation.handleDataMessage(fruitRecords, clientID)
	ack()
}

func (aggregation *Aggregation) processEOF(clientID int64, fruits []fruititem.FruitItem) error {
	if len(fruits) == 0 {
		slog.Info("Dont send fruits", "clientID", clientID)
	} else {
		message, err := inner.SerializeMessage(fruits, clientID)
		if err != nil {
			slog.Debug("While serializing top fruits message to join", "err", err, "clientID", clientID)
			return err
		}
		slog.Info("Fruits sent", "fruitsTop", fruits, "clientID", clientID)
		err = aggregation.outputQueue.Send(*message)
		if err != nil {
			slog.Error("While sending top fruits message to join", "err", err, "clientID", clientID)
			return err
		}
	}
	return nil
}

func (aggregation *Aggregation) processFF(clientID int64) error {
	message, err := inner.SerializeMessage([]fruititem.FruitItem{}, clientID)
	if err != nil {
		slog.Debug("While serializing top fruits message to join", "err", err, "clientID", clientID)
		return err
	}
	err = aggregation.outputQueue.Send(*message)
	if err != nil {
		slog.Error("While sending top fruits message to join", "err", err, "clientID", clientID)
		return err
	}
	slog.Info("Sent EOF to join", "clientID", clientID)
	return nil
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)
	aggregation.counterEOFs[clientID]++
	if aggregation.counterEOFs[clientID] != aggregation.config.SumAmount {
		return nil
	}

	slog.Info("All EOFs received, calculating top and sending", "clientID", clientID)
	if fruitsMap, ok := aggregation.clientFruits[clientID]; ok {
		top := aggregation.buildFruitTop(fruitsMap)
		aggregation.processEOF(clientID, top)
	} else {
		slog.Info("Client dont have fruits to build top", "clientID", clientID)
	}

	err := aggregation.processFF(clientID)
	if err != nil {
		// NACK?
		return nil
	}
	delete(aggregation.clientFruits, clientID)
	delete(aggregation.counterEOFs, clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID int64) {
	if _, ok := aggregation.clientFruits[clientID]; !ok {
		slog.Info("New client", "clientID", clientID)
		aggregation.clientFruits[clientID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.clientFruits[clientID][fruitRecord.Fruit]; ok {
			aggregation.clientFruits[clientID][fruitRecord.Fruit] = aggregation.clientFruits[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.clientFruits[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitsMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitsMap))
	for _, item := range fruitsMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.config.TopSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
