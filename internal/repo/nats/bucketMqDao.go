package nats

import (
	"encoding/json"
	"github.com/Nubes3/common/models/arangodb"
	"github.com/Nubes3/common/models/nats"
	"github.com/Nubes3/common/utils"
	"time"
)

func FindBucketById(id string) (*arangodb.Bucket, error) {
	message := nats.Msg{
		ReqType:   nats.GetById,
		Data:      id,
		ExtraData: nil,
	}
	messageJson, _ := json.Marshal(message)
	rawRep, err := nats.Nc.Request(nats.BucketSubj, messageJson, time.Second*10)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.Timeout,
		}
	}

	var rep nats.MsgResponse
	_ = json.Unmarshal(rawRep.Data, &rep)
	if rep.IsErr {
		return nil, &utils.ModelError{
			Msg:     rep.Data,
			ErrType: utils.Other,
		}
	}

	var bucket arangodb.Bucket
	err = json.Unmarshal([]byte(rep.Data), &bucket)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.Other,
		}
	}

	return &bucket, nil
}
