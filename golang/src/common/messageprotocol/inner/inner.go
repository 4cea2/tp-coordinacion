package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageClient struct {
	ClientId int64         `json:"client_id"`
	Data     []interface{} `json:"data"`
}

const (
	TypeEOF    = iota // 0
	TypeAckEOF        // 1 // 2
)

type ControlMessage struct {
	Type      uint8 `json:"type"`
	ClientID  int64 `json:"client_id"`
	OriginID  int   `json:"origin_id"`
	ReplyToID int   `json:"reply_to_id"`
}

func serializeJson(messageClient MessageClient) ([]byte, error) {
	return json.Marshal(messageClient)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(fruitRecords []fruititem.FruitItem, clientID int64) (*middleware.Message, error) {
	fruitData := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		fruitData = append(fruitData, datum)
	}

	msgClient := MessageClient{clientID, fruitData}

	body, err := serializeJson(msgClient)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) ([]fruititem.FruitItem, int64, bool, error) {
	var msgClient MessageClient
	err := json.Unmarshal([]byte(message.Body), &msgClient)
	if err != nil {
		return nil, 0, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range msgClient.Data {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, 0, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, 0, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, 0, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, msgClient.ClientId, len(fruitRecords) == 0, nil
}

func SerializeControlMessage(ctrlMsg ControlMessage) (*middleware.Message, error) {
	body, err := json.Marshal(ctrlMsg)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeControlMessage(message *middleware.Message) (*ControlMessage, error) {
	var ctrlMsg ControlMessage
	err := json.Unmarshal([]byte(message.Body), &ctrlMsg)
	if err != nil {
		return nil, err
	}

	return &ctrlMsg, nil
}
