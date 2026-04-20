package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue   middleware.Middleware
	outputQueue  middleware.Middleware
	clientFruits map[int64]map[string]fruititem.FruitItem
	config       JoinConfig
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{inputQueue: inputQueue,
		outputQueue:  outputQueue,
		clientFruits: map[int64]map[string]fruititem.FruitItem{},
		config:       config,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	fruitRecords, clientID, isEof, _ := inner.DeserializeMessage(&msg)

	if isEof {
		slog.Info("Received End Of Records message", "clientID", clientID)
		fruitsTop := join.buildFruitTop(join.clientFruits[clientID]) // y si el cliente no me manda nada, solo un eof?
		slog.Info("top fruits per client", "fruits", fruitsTop, "clientID", clientID)
		message, err := inner.SerializeMessage(fruitsTop, clientID)
		if err != nil {
			slog.Error("While serialize top", "err", err)
		}
		if err = join.outputQueue.Send(*message); err != nil {
			slog.Error("While sending top", "err", err)
		}
	} else {
		if _, ok := join.clientFruits[clientID]; !ok {
			join.clientFruits[clientID] = map[string]fruititem.FruitItem{}
		}

		for _, fruitRecord := range fruitRecords {
			if _, ok := join.clientFruits[clientID][fruitRecord.Fruit]; ok {
				join.clientFruits[clientID][fruitRecord.Fruit] = join.clientFruits[clientID][fruitRecord.Fruit].Sum(fruitRecord)
			} else {
				join.clientFruits[clientID][fruitRecord.Fruit] = fruitRecord
			}
		}
	}
}

func (join *Join) buildFruitTop(fruitsMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitsMap))
	for _, item := range fruitsMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.config.TopSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
