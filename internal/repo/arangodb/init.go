package arango

import (
	"context"
	common "github.com/Nubes3/common/models/arangodb"
	arangoDriver "github.com/arangodb/go-driver"
)

var (
	fileMetadataCol arangoDriver.Collection
)

func init() {
	ctx, cancel := context.WithTimeout(context.Background(), common.ContextExpiredTime)
	defer cancel()

	exist, err := common.ArangoDb.CollectionExists(ctx, "users")
	if err != nil {
		panic(err)
	}

	if !exist {
		fileMetadataCol, _ = common.ArangoDb.CreateCollection(ctx, "users", &arangoDriver.CreateCollectionOptions{})
	} else {
		fileMetadataCol, _ = common.ArangoDb.Collection(ctx, "users")
	}
}
