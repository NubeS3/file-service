package nats

import (
	"encoding/json"
	"github.com/Nubes3/common/models/arangodb"
	"github.com/Nubes3/common/models/nats"
	"github.com/Nubes3/common/utils"
	"strconv"
	"time"
)

func FindFolderByFullpath(fullname string) (*arangodb.Folder, error) {
	message := nats.Msg{
		ReqType:   nats.GetByParams,
		Data:      fullname,
		ExtraData: nil,
	}
	messageJson, _ := json.Marshal(message)
	rawRep, err := nats.Nc.Request(nats.FolderSubj, messageJson, time.Second*10)
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

	var folder arangodb.Folder
	err = json.Unmarshal([]byte(rep.Data), &folder)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.Other,
		}
	}

	return &folder, nil
}

func InsertFile(fid, fname, parentId string, isHidden bool) (*arangodb.Folder, error) {
	message := nats.Msg{
		ReqType:   nats.Add,
		Data:      fid,
		ExtraData: []string{fname, parentId, strconv.FormatBool(isHidden)},
	}
	messageJson, _ := json.Marshal(message)
	rawRep, err := nats.Nc.Request(nats.FolderSubj, messageJson, time.Second*10)
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

	var folder arangodb.Folder
	err = json.Unmarshal([]byte(rep.Data), &folder)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.Other,
		}
	}

	return &folder, nil
}

func UpdateHiddenStatusOfFolderChild(path, fid, name string, hiddenStatus bool) (*arangodb.Folder, error) {
	message := nats.Msg{
		ReqType:   nats.Update,
		Data:      fid,
		ExtraData: []string{path, name, strconv.FormatBool(hiddenStatus)},
	}
	messageJson, _ := json.Marshal(message)
	rawRep, err := nats.Nc.Request(nats.FolderSubj, messageJson, time.Second*10)
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

	var folder arangodb.Folder
	err = json.Unmarshal([]byte(rep.Data), &folder)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.Other,
		}
	}

	return &folder, nil
}
