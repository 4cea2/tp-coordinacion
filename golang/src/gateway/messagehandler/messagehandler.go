package messagehandler

import (
	"log/slog"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientID int64
}

func NewMessageHandler() MessageHandler {
	id := time.Now().UnixNano()
	return MessageHandler{clientID: id}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(data, messageHandler.clientID)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(data, messageHandler.clientID)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, clientID, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	slog.Info("Deserializing ", "fruits", fruitRecords, "clientID", clientID)

	if clientID != messageHandler.clientID {
		slog.Info("ClientID dismatch, skipping...")
		return nil, nil
	}
	return fruitRecords, nil
}
