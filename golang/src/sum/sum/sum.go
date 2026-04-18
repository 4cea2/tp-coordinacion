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

	nameExchangeSums := config.SumPrefix
	exchangeSums, err := middleware.CreateExchangeMiddleware(nameExchangeSums, []string{"broadcast"}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	slog.Info("Nombre exchange and keys", "exchange", nameExchangeSums, "keys", []string{"broadcast"})
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
		sumAmount:      config.SumAmount - 1,
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
	// RACE CONDITION CON handleMessage, puede estar guardando una fruta mientras estoy enviando estas (deberia fallar los test, no quedarse colgado)
	// Tambien con el counters eofs?
	fruitRecords, clientID, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		// Nack?
		slog.Error("While deserializing FF message from", "clientID", clientID)
		return
	}

	// Tengo que distinguir de FF o EOF
	if len(fruitRecords) == 1 {
		if fruitRecords[0].Fruit == fmt.Sprintf("%d", sum.id) {
			slog.Info("No me interesa mi propio FF")
			return
		}
		slog.Info("Final fruit recibida")
		// Si es FF, mando las frutas actuales del cliente, y mando EOF
		fruitItemMap, ok := sum.clientFruits[clientID]
		if !ok {
			slog.Info("Ya borre, no hago nada")
			// Si me llega esto cuando estoy en el caso borde mencionado abajo, no deberia retornar...
			return
		}
		err := sum.sendFruitsToOutput(clientID, fruitItemMap)
		if err != nil {
			// Tengo que nack?
			return
		}
		eofMessage := []fruititem.FruitItem{}
		err = sum.sendMessageToExchangeSums(clientID, eofMessage)
		if err != nil {
			// nack?
			return
		}
		delete(sum.clientFruits, clientID)
	} else {
		slog.Info("Recibi EOF de otros sums")
		// Si es EOF, verifico que me lleguen los que espere (si soy el que envio FF)
		_, ok := sum.clientFruits[clientID]
		if !ok {
			// No fui el que el FF, no hago nada
			slog.Info("No fui el que envio el FF")
			return
		}

		err := sum.processEOF(clientID)
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
	fruitItemMap, ok := sum.clientFruits[clientID]

	if !ok {
		// Puede darse el caso en que los demas sums hayan procesado todas la frutas del cliente
		// Por ende nunca guardarste una fruta en este sum y justo te llego este EOF
	}
	err := sum.sendFruitsToOutput(clientID, fruitItemMap)
	if err != nil {
		return err
	}

	fruitFinalMessage := sum.createFruitFinalMessage()
	err = sum.sendMessageToExchangeSums(clientID, fruitFinalMessage)
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

func (sum *Sum) createFruitFinalMessage() []fruititem.FruitItem {
	fruitFinal := fruititem.FruitItem{Fruit: fmt.Sprintf("%d", sum.id), Amount: uint32(0)}
	fruitFinalMessage := []fruititem.FruitItem{}
	fruitFinalMessage = append(fruitFinalMessage, fruitFinal)
	return fruitFinalMessage
}

func (sum *Sum) processEOF(clientID int64) error {
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
		delete(sum.clientFruits, clientID)
		sum.counterEOFs = 0
	}

	return nil
}
