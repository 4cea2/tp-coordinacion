package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

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
	topSize       int
	exchangeAggs  middleware.Middleware
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

	exchangeAggs, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{"broadcast"}, connSettings)
	if err != nil {
		outputQueue.Close()
		inputExchange.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		clientFruits:  map[int64]map[string]fruititem.FruitItem{},
		counterEOFs:   map[int64]int{},
		topSize:       config.TopSize,
		exchangeAggs:  exchangeAggs,
		config:        config,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.exchangeAggs.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessageExchange(msg, ack, nack)
	})

	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessageExchange(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	fruitRecords, clientID, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		// Nack?
		slog.Error("While deserializing message from exchange", "clientID", clientID)
		return
	}

	fruitName := fruitRecords[0].Fruit
	aggId := fruitRecords[0].Amount
	if fruitName == "EOF" {
		err := aggregation.processEOF(clientID, aggId)
		if err != nil {
			// nack?
			return
		}
	} else if fruitName == "FF" {
		if fmt.Sprintf("%d", aggId) != fmt.Sprintf("%d", aggregation.config.Id) {
			// Si no fui el que envio el EOF, no me interesa
			return
		}
		err := aggregation.processFF(clientID, fruitRecords[1:])
		if err != nil {
			// NACK?
			return
		}
	} else {
		slog.Info("Received unexpected message from exchangeAggs", "clientID", clientID)
	}
}

// Me envio mi propio top
func (aggregation *Aggregation) processEOF(clientID int64, aggId uint32) error {
	_, ok := aggregation.clientFruits[clientID]

	fruitsTop := []fruititem.FruitItem{fruititem.FruitItem{Fruit: "FF", Amount: aggId}}
	if ok {
		top := aggregation.buildFruitTop(clientID)
		for _, fruit := range top {
			fruitsTop = append(fruitsTop, fruit)
		}
		delete(aggregation.clientFruits, clientID)
	}

	finalFruitMessage, err := inner.SerializeMessage(fruitsTop, clientID)
	if err != nil {
		slog.Debug("While serializing message to other aggs", "err", err, "fruitMessage", finalFruitMessage, "clientID", clientID)
		return err
	}
	err = aggregation.exchangeAggs.Send(*finalFruitMessage)
	if err != nil {
		// nack?
		return err
	}
	return nil
}

func (aggregation *Aggregation) processFF(clientID int64, fruits []fruititem.FruitItem) error {
	aggregation.counterEOFs[clientID] += 1
	_, ok := aggregation.clientFruits[clientID]
	if !ok {
		// Siempre deberia entrar aca porque la elimino antes...
		aggregation.clientFruits[clientID] = map[string]fruititem.FruitItem{}
	}
	for _, fruit := range fruits {
		aggregation.clientFruits[clientID][fruit.Fruit] = fruit // No piso a ninguna fruta porque las que me llegan son todas nuevas
	}

	if aggregation.counterEOFs[clientID] == aggregation.config.AggregationAmount {
		fruitTops := aggregation.buildFruitTop(clientID)
		message, err := inner.SerializeMessage(fruitTops, clientID)
		if err != nil {
			slog.Debug("While serializing top fruits message to join", "err", err, "clientID", clientID)
			return err
		}
		err = aggregation.outputQueue.Send(*message)
		if err != nil {
			slog.Error("While sending top fruits message to join", "err", err, "clientID", clientID)
			return err
		}

		delete(aggregation.counterEOFs, clientID)
		delete(aggregation.clientFruits, clientID)
	}
	return nil
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, clientID, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err, "clientID", clientID)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err, clientID)
		}
		return
	}

	aggregation.handleDataMessage(fruitRecords, clientID)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID int64) error {
	slog.Info("Received End Of Records message", "clientID", clientID)

	fruit := []fruititem.FruitItem{{Fruit: "EOF", Amount: uint32(aggregation.config.Id)}}
	message, err := inner.SerializeMessage(fruit, clientID)
	if err != nil {
		slog.Debug("While serializing top message", "err", err, "clientID", clientID)
		return err
	}
	if err := aggregation.exchangeAggs.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err, "clientID", clientID)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID int64) {
	if _, ok := aggregation.clientFruits[clientID]; !ok {
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

func (aggregation *Aggregation) buildFruitTop(clientID int64) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.clientFruits[clientID]))
	for _, item := range aggregation.clientFruits[clientID] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
