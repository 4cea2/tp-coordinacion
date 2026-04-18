package sum

import (
	"fmt"
	"log/slog"

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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	exchangeSums   middleware.Middleware
	sumAmount      int
	id             int
	counterEOFs    int
	clientFruits   map[int64]map[string]fruititem.FruitItem
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

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		exchangeSums.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		exchangeSums:   exchangeSums,
		sumAmount:      config.SumAmount,
		id:             config.Id,
		counterEOFs:    0,
		clientFruits:   map[int64]map[string]fruititem.FruitItem{},
	}, nil
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
	fruitRecords, clientID, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		// Nack?
		slog.Error("While deserializing FF message from exchange", "clientID", clientID)
		return
	}
	sumId := fruitRecords[0].Fruit
	if len(fruitRecords) == 1 {
		_, ok := sum.clientFruits[clientID]
		if !ok {
			// Si me llega esto, estoy en el caso borde mencionado abajo, no deberia retornar?
			return
		}
		err := sum.processEOF(clientID, sumId)
		if err != nil {
			// nack?
			return
		}
	} else {
		if sumId != fmt.Sprintf("%d", sum.id) {
			return
		}
		err := sum.processFF(clientID)
		if err != nil {
			// NACK?
			return
		}
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

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
	_, ok := sum.clientFruits[clientID]

	if !ok {
		// Puede darse el caso en que los demas sums hayan procesado todas las frutas del cliente
		// Por ende nunca guardaste una fruta en esta replica, y justo te llego este EOF
	}

	eofMessage := []fruititem.FruitItem{}
	eofFruitMessage := fruititem.FruitItem{Fruit: fmt.Sprintf("%d", sum.id), Amount: uint32(0)}
	eofMessage = append(eofMessage, eofFruitMessage)
	err := sum.sendMessageToExchangeSums(clientID, eofMessage)
	if err != nil {
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem, clientID int64) error {
	if _, ok := sum.clientFruits[clientID]; !ok {
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
		message, err := inner.SerializeMessage(fruitRecord, clientID)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) sendMessageToExchangeSums(clientID int64, fruitMessage []fruititem.FruitItem) error {
	message, err := inner.SerializeMessage(fruitMessage, clientID)
	if err != nil {
		slog.Debug("While serializing message to other sums", "err", err, "fruitMessage", fruitMessage, "clientID", clientID)
		return err
	}
	if err := sum.exchangeSums.Send(*message); err != nil {
		slog.Debug("While sending message to other sums", "err", err, "fruitMessage", fruitMessage, "clientID", clientID)
		return err
	}

	return nil
}

func (sum *Sum) createFruitFinalMessage(sumId string) []fruititem.FruitItem {
	fruitFinal1 := fruititem.FruitItem{Fruit: sumId, Amount: uint32(0)}
	fruitFinal2 := fruititem.FruitItem{Fruit: sumId, Amount: uint32(0)}
	fruitFinalMessage := []fruititem.FruitItem{}
	fruitFinalMessage = append(fruitFinalMessage, fruitFinal1)
	fruitFinalMessage = append(fruitFinalMessage, fruitFinal2)
	return fruitFinalMessage
}

func (sum *Sum) processFF(clientID int64) error {
	sum.counterEOFs += 1
	if sum.counterEOFs == sum.sumAmount {
		eofMessage := []fruititem.FruitItem{}
		message, err := inner.SerializeMessage(eofMessage, clientID)
		if err != nil {
			slog.Debug("While serializing EOF message", "err", err, "clientID", clientID)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err, "clientID", clientID)
			return err
		}
		delete(sum.clientFruits, clientID) // Si soy el mismo que envio EOF, no va a hacer nada, ya que lo elimine en processFF
		sum.counterEOFs = 0
	}

	return nil
}

func (sum *Sum) processEOF(clientID int64, sumId string) error {
	err := sum.sendFruitsToOutput(clientID, sum.clientFruits[clientID])
	if err != nil {
		// Tengo que nack?
		return err
	}
	finalFruitMessage := sum.createFruitFinalMessage(sumId)
	err = sum.sendMessageToExchangeSums(clientID, finalFruitMessage)
	if err != nil {
		// nack?
		return err
	}
	delete(sum.clientFruits, clientID)

	return nil
}
