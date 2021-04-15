package arango

import (
	"context"
	"github.com/Nubes3/common/models/arangodb"
	"github.com/Nubes3/common/models/seaweedfs"
	"github.com/Nubes3/common/utils"
	"github.com/Nubes3/file-service/internal/repo/nats"
	"github.com/arangodb/go-driver"
	"io"
	"time"
)

func saveFileMetadata(fid string, bid string,
	path string, name string, isHidden bool,
	contentType string, size int64, expiredDate time.Time) (*arangodb.FileMetadata, error) {
	uploadedTime := time.Now()
	f, err := nats.FindFolderByFullpath(path)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     "folder not found",
			ErrType: utils.NotFound,
		}
	}

	doc := arangodb.FileMetadataRes{
		FileId:       fid,
		BucketId:     bid,
		Path:         path,
		Name:         name,
		ContentType:  contentType,
		Size:         size,
		IsHidden:     isHidden,
		IsDeleted:    false,
		DeletedDate:  time.Time{},
		UploadedDate: uploadedTime,
		ExpiredDate:  expiredDate,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	meta, err := fileMetadataCol.CreateDocument(ctx, doc)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	_, err = nats.InsertFile(meta.Key, doc.Name, f.Id, isHidden)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     "insert file to folder failed",
			ErrType: utils.DbError,
		}
	}

	//LOG UPLOAD SUCCESS
	//_ = nats.SendUploadSuccessFileEvent(meta.Key, doc.FileId, doc.Name, doc.Size,
	//	doc.BucketId, doc.ContentType, doc.UploadedDate, doc.Path, doc.IsHidden)

	return &arangodb.FileMetadata{
		Id:           meta.Key,
		FileId:       doc.FileId,
		BucketId:     doc.BucketId,
		Path:         doc.Path,
		Name:         doc.Name,
		ContentType:  doc.ContentType,
		Size:         doc.Size,
		IsHidden:     doc.IsHidden,
		IsDeleted:    doc.IsDeleted,
		DeletedDate:  doc.DeletedDate,
		UploadedDate: doc.UploadedDate,
		ExpiredDate:  doc.ExpiredDate,
	}, nil
}

func FindMetadataByBid(bid string, limit int64, offset int64, showHidden bool) ([]arangodb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	var query string
	if showHidden {
		query = "FOR fm IN fileMetadata FILTER fm.bucket_id == @bid " +
			"LIMIT @offset, @limit RETURN fm"
	} else {
		query = "FOR fm IN fileMetadata FILTER fm.bucket_id == @bid " +
			"AND fm.is_hidden == false LIMIT @offset, @limit RETURN fm"
	}

	bindVars := map[string]interface{}{
		"bid":    bid,
		"offset": offset,
		"limit":  limit,
	}

	fileMetadatas := []arangodb.FileMetadata{}
	fileMetadata := arangodb.FileMetadata{}

	cursor, err := arangodb.ArangoDb.Query(ctx, query, bindVars)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}
	defer cursor.Close()

	for {
		meta, err := cursor.ReadDocument(ctx, &fileMetadata)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, &utils.ModelError{
				Msg:     err.Error(),
				ErrType: utils.DbError,
			}
		}
		fileMetadata.Id = meta.Key
		fileMetadatas = append(fileMetadatas, fileMetadata)
	}

	return fileMetadatas, nil
}

func FindMetadataByFilename(path string, name string, bid string) (*arangodb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	query := "FOR fm IN fileMetadata FILTER fm.bucket_id == @bid AND fm.path == @path AND fm.name == @name LIMIT 1 RETURN fm"
	bindVars := map[string]interface{}{
		"bid":  bid,
		"path": path,
		"name": name,
	}

	fm := arangodb.FileMetadataRes{}
	cursor, err := arangodb.ArangoDb.Query(ctx, query, bindVars)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}
	defer cursor.Close()

	var retMeta arangodb.FileMetadata
	for {
		meta, err := cursor.ReadDocument(ctx, &fm)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, &utils.ModelError{
				Msg:     err.Error(),
				ErrType: utils.DbError,
			}
		}

		retMeta = arangodb.FileMetadata{
			Id:           meta.Key,
			FileId:       fm.FileId,
			BucketId:     fm.BucketId,
			Path:         fm.Path,
			Name:         fm.Name,
			ContentType:  fm.ContentType,
			Size:         fm.Size,
			IsHidden:     fm.IsHidden,
			IsDeleted:    fm.IsDeleted,
			DeletedDate:  fm.DeletedDate,
			UploadedDate: fm.UploadedDate,
			ExpiredDate:  fm.ExpiredDate,
		}
	}

	if retMeta.Id == "" {
		return nil, &utils.ModelError{
			Msg:     "not found",
			ErrType: utils.NotFound,
		}
	}

	return &retMeta, nil
}

func FindMetadataByFid(fid string) (*arangodb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	query := "FOR fm IN fileMetadata FILTER fm.fid == @fid LIMIT 1 RETURN fm"
	bindVars := map[string]interface{}{
		"fid": fid,
	}

	fm := arangodb.FileMetadataRes{}
	cursor, err := arangodb.ArangoDb.Query(ctx, query, bindVars)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}
	defer cursor.Close()

	var retMeta arangodb.FileMetadata
	for {
		meta, err := cursor.ReadDocument(ctx, &fm)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, &utils.ModelError{
				Msg:     err.Error(),
				ErrType: utils.DbError,
			}
		}

		retMeta = arangodb.FileMetadata{
			Id:           meta.Key,
			FileId:       fm.FileId,
			BucketId:     fm.BucketId,
			Path:         fm.Path,
			Name:         fm.Name,
			ContentType:  fm.ContentType,
			Size:         fm.Size,
			IsHidden:     fm.IsHidden,
			IsDeleted:    fm.IsDeleted,
			DeletedDate:  fm.DeletedDate,
			UploadedDate: fm.UploadedDate,
			ExpiredDate:  fm.ExpiredDate,
		}
	}

	return &retMeta, nil
}

func FindMetadataById(fid string) (*arangodb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	var data arangodb.FileMetadataRes
	meta, err := fileMetadataCol.ReadDocument(ctx, fid, &data)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	if data.IsDeleted || data.ExpiredDate.Before(time.Now()) {
		return nil, &utils.ModelError{
			Msg:     "file not found",
			ErrType: utils.NotFound,
		}
	}

	return &arangodb.FileMetadata{
		Id:           meta.Key,
		FileId:       data.FileId,
		BucketId:     data.BucketId,
		Path:         data.Path,
		Name:         data.Name,
		ContentType:  data.ContentType,
		Size:         data.Size,
		IsHidden:     data.IsHidden,
		IsDeleted:    data.IsDeleted,
		DeletedDate:  data.DeletedDate,
		UploadedDate: data.UploadedDate,
		ExpiredDate:  data.ExpiredDate,
	}, nil
}

func SaveFile(reader io.Reader, bid string,
	path string, name string, isHidden bool,
	contentType string, size int64, ttl time.Duration) (*arangodb.FileMetadata, error) {
	//CHECK BUCKET ID AND NAME
	_, err := nats.FindBucketById(bid)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	if ttl == time.Duration(0) {
		ttl = time.Hour * 24 * 365 * 10
	}

	//CHECK DUP FILE NAME
	_, err = FindMetadataByFilename(path, name, bid)
	if err == nil {
		return nil, &utils.ModelError{
			Msg:     "duplicate file",
			ErrType: utils.Duplicated,
		}
	}

	//LOG STAGING
	//_ = nats.SendStagingFileEvent(name, size, bid, contentType, path, isHidden)

	meta, err := seaweedfs.UploadFile(name, size, reader)
	if err != nil {
		return nil, err
	}

	return saveFileMetadata(meta.FileID, bid, path, name, isHidden, contentType, size, time.Now().Add(ttl))
}

func GetFile(bid string, path, name string, callback func(reader io.Reader, metadata *arangodb.FileMetadata) error) error {
	meta, err := FindMetadataByFilename(path, name, bid)
	if err != nil {
		return &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	//CHECK EXPIRED TIME

	//CHECK FILE DELETE

	err = seaweedfs.DownloadFile(meta.FileId, func(reader io.Reader) error {
		return callback(reader, meta)
	})

	if err != nil {
		return &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.FsError,
		}
	}

	return nil
}

func GetFileByFid(fid string, callback func(reader io.Reader, metadata *arangodb.FileMetadata) error) error {
	fileMeta, err := FindMetadataById(fid)
	if err != nil {
		return &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	err = seaweedfs.DownloadFile(fileMeta.FileId, func(reader io.Reader) error {
		return callback(reader, fileMeta)
	})

	if err != nil {
		return &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.FsError,
		}
	}

	return nil
}

func GetFileByFidIgnoreQueryMetadata(fid string, callback func(reader io.Reader) error) error {
	err := seaweedfs.DownloadFile(fid, callback)

	if err != nil {
		return &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.FsError,
		}
	}

	return nil
}

func ToggleHidden(fullpath string, isHidden bool) (*arangodb.FileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*arangodb.ContextExpiredTime)
	defer cancel()

	query := "FOR fm IN fileMetadata FILTER fm.path == @fullpath UPDATE fm WITH { is_hidden: @isHidden} IN fileMetadata RETURN NEW"
	bindVars := map[string]interface{}{
		"fullpath": fullpath,
		"isHidden": isHidden,
	}

	cursor, err := arangodb.ArangoDb.Query(ctx, query, bindVars)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}
	defer cursor.Close()

	fileMetadata := arangodb.FileMetadata{}
	for {
		meta, err := cursor.ReadDocument(ctx, &fileMetadata)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, &utils.ModelError{
				Msg:     err.Error(),
				ErrType: utils.DbError,
			}
		}
		fileMetadata.Id = meta.Key
	}

	if fileMetadata.Id == "" {
		return nil, &utils.ModelError{
			Msg:     "folder not found",
			ErrType: utils.NotFound,
		}
	}

	_, err = nats.UpdateHiddenStatusOfFolderChild(fileMetadata.Path, fileMetadata.Id,
		fileMetadata.Name, fileMetadata.IsHidden)
	if err != nil {
		return nil, &utils.ModelError{
			Msg:     err.Error(),
			ErrType: utils.DbError,
		}
	}

	return &fileMetadata, nil
}
