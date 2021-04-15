package aggregate

import (
	"github.com/Nubes3/common/models/arangodb"
	"github.com/Nubes3/common/utils"
	"github.com/Nubes3/file-service/internal/repo/arangodb"
	"github.com/Nubes3/file-service/internal/repo/nats"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"strconv"
	"time"
)

func GetAllFileWithAccessKey(c *gin.Context) {
	limit, err := strconv.ParseInt(c.DefaultQuery("limit", "10"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid limit format",
		})

		return
	}
	offset, err := strconv.ParseInt(c.DefaultQuery("offset", "0"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid offset format",
		})

		return
	}

	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /files/all:",
		//	"Unknown Error")
		return
	}
	accessKey := key.(*arangodb.AccessKey)
	var isGetFileListPerm bool
	for _, perm := range accessKey.Permissions {
		if perm == "GetFileList" {
			isGetFileListPerm = true
			break
		}
	}

	if !isGetFileListPerm {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "not have permission",
		})
		return
	}

	res, err := arango.FindMetadataByBid(accessKey.BucketId, limit, offset, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at files/all:",
		//	"Db Error")
		return
	}

	c.JSON(http.StatusOK, res)
}

func GetAllFileIncludeHiddenAccessKey(c *gin.Context) {
	limit, err := strconv.ParseInt(c.DefaultQuery("limit", "10"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid limit format",
		})

		return
	}
	offset, err := strconv.ParseInt(c.DefaultQuery("offset", "0"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid offset format",
		})

		return
	}

	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /files/hidden/all:",
		//	"Unknown Error")
		return
	}
	accessKey := key.(*arangodb.AccessKey)
	var isGetFileListHiddenPerm bool
	for _, perm := range accessKey.Permissions {
		if perm == "GetFileListHidden" {
			isGetFileListHiddenPerm = true
			break
		}
	}

	if !isGetFileListHiddenPerm {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "not have permission",
		})
		return
	}

	res, err := arango.FindMetadataByBid(accessKey.BucketId, limit, offset, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/hidden/all:",
		//	"Db Error")
		return
	}

	c.JSON(http.StatusOK, res)
}

func UploadFileWithAccessKey(c *gin.Context) {
	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /files/upload:",
		//	"Unknown Error")
		return
	}

	accessKey := key.(*arangodb.AccessKey)
	var isUploadPerm bool
	for _, perm := range accessKey.Permissions {
		if perm == "Upload" {
			isUploadPerm = true
			break
		}
	}

	if !isUploadPerm {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "not have permission",
		})
		return
	}

	bucket, err := nats.FindBucketById(accessKey.BucketId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	uploadFile, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	queryPath := c.DefaultPostForm("path", "/")
	path := utils.StandardizedPath("/"+bucket.Name+"/"+queryPath, true)

	fileName := c.DefaultPostForm("name", uploadFile.Filename)
	//newPath := bucket.Name + path + fileName

	fileContent, err := uploadFile.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("open file failed at /files/upload:",
		//	"File Error")
		return
	}

	fileSize := uploadFile.Size
	ttlStr := c.DefaultPostForm("ttl", "0")
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
	}

	isHiddenStr := c.DefaultPostForm("hidden", "false")
	isHidden, err := strconv.ParseBool(isHiddenStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})

		return
	}

	cType, err := utils.GetFileContentType(fileContent)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "unknown file content type",
		})

		return
	}

	res, err := arango.SaveFile(fileContent, accessKey.BucketId, path, fileName, isHidden,
		cType, fileSize, time.Duration(ttl)*time.Second)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.Duplicated {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": err.Error(),
				})

				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent("db error at /files/upload: "+err.Error(),
		//	"File Error")
		return
	}

	//LOG
	//_ = nats.SendUploadFileEvent(res.Id, res.FileId, res.Name, res.Size,
	//	res.BucketId, res.ContentType, res.UploadedDate, res.Path, res.IsHidden)

	c.JSON(http.StatusOK, res)
}

func DownloadFileByIdWithAccessKey(c *gin.Context) {
	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /files/download:",
		//	"Unknown Error")
		return
	}
	accessKey := key.(*arangodb.AccessKey)
	var isDownloadPerm bool
	for _, perm := range accessKey.Permissions {
		if perm == "Download" {
			isDownloadPerm = true
			break
		}
	}
	if !isDownloadPerm {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "not have permission",
		})
		return
	}
	fid := c.DefaultQuery("fileId", "")

	fileMeta, err := arango.FindMetadataById(fid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "file not found",
		})

		return
	}

	if fileMeta.IsHidden {
		var isDownloadHiddenPerm bool
		for _, perm := range accessKey.Permissions {
			if perm == "DownloadHidden" {
				isDownloadHiddenPerm = true
				break
			}
		}
		if !isDownloadHiddenPerm {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "file not found",
			})

			return
		}
	}

	err = arango.GetFileByFidIgnoreQueryMetadata(fileMeta.FileId, func(reader io.Reader) error {
		if fileMeta.BucketId != accessKey.BucketId {
			return &utils.ModelError{
				Msg:     "invalid bucket",
				ErrType: utils.Invalid,
			}
		}

		extraHeaders := map[string]string{
			"Content-Disposition": `attachment; filename=` + fileMeta.Name,
		}

		c.DataFromReader(http.StatusOK, fileMeta.Size, fileMeta.ContentType, reader, extraHeaders)

		//LOG
		//_ = nats.SendDownloadFileEvent(fileMeta.Id, fileMeta.FileId, fileMeta.Name, fileMeta.Size,
		//	fileMeta.BucketId, fileMeta.ContentType, fileMeta.UploadedDate, fileMeta.Path, fileMeta.IsHidden)

		return nil
	})

	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.Invalid {
				c.JSON(http.StatusForbidden, gin.H{
					"error": err.Error(),
				})

				return
			}
		}

		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusNotFound, gin.H{
					"error": "file not found",
				})

				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("download failed: "+err.Error()+" at /files/download:",
		//	"File Error")
		return
	}
}

func DownloadFileByPathWithAccessKey(c *gin.Context) {
	fullpath := c.Param("fullpath")
	fullpath = utils.StandardizedPath(fullpath, true)
	bucketName := utils.GetBucketName(fullpath)
	parentPath := utils.GetParentPath(fullpath)
	fileName := utils.GetFileName(fullpath)

	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /files/upload:",
		//	"Unknown Error")
		return
	}
	accessKey := key.(*arangodb.AccessKey)

	bucket, err := nats.FindBucketById(accessKey.BucketId)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "bucket not found",
		})

		return
	}

	if bucket.Name != bucketName {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid bucket name",
		})

		return
	}

	fileMeta, err := arango.FindMetadataByFilename(parentPath, fileName, *bucket.Id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "file not found",
		})

		return
	}

	if fileMeta.IsHidden {
		var isDownloadHiddenPerm bool
		for _, perm := range accessKey.Permissions {
			if perm == "DownloadHidden" {
				isDownloadHiddenPerm = true
				break
			}
		}
		if !isDownloadHiddenPerm {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "file not found",
			})

			return
		}
	}

	err = arango.GetFileByFidIgnoreQueryMetadata(fileMeta.FileId, func(reader io.Reader) error {
		if fileMeta.BucketId != accessKey.BucketId {
			return &utils.ModelError{
				Msg:     "invalid bucket",
				ErrType: utils.Invalid,
			}
		}

		extraHeaders := map[string]string{
			"Content-Disposition": `attachment; filename=` + fileMeta.Name,
		}

		c.DataFromReader(http.StatusOK, fileMeta.Size, fileMeta.ContentType, reader, extraHeaders)

		//LOG
		//_ = nats.SendDownloadFileEvent(fileMeta.Id, fileMeta.FileId, fileMeta.Name, fileMeta.Size,
		//	fileMeta.BucketId, fileMeta.ContentType, fileMeta.UploadedDate, fileMeta.Path, fileMeta.IsHidden)

		return nil
	})

	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.Invalid {
				c.JSON(http.StatusForbidden, gin.H{
					"error": err.Error(),
				})

				return
			}
		}

		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusNotFound, gin.H{
					"error": "file not found",
				})

				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("download failed: "+err.Error()+" at /files/download:",
		//	"File Error")
		return
	}
}

func ToggleHiddenByAccessKey(c *gin.Context) {
	qIsHidden := c.DefaultQuery("hidden", "false")
	qName := c.DefaultQuery("name", "")
	qPath := c.DefaultQuery("path", "")

	key, ok := c.Get("accessKey")
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("accessKey not found in authenticate at /accessKey/files/hidden:",
		//	"Unknown Error")
		return
	}
	accessKey := key.(*arangodb.AccessKey)

	var isMarkHiddenPerm bool
	for _, perm := range accessKey.Permissions {
		if perm == "MarkHidden" {
			isMarkHiddenPerm = true
			break
		}
	}

	if !isMarkHiddenPerm {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "not have permission",
		})
		return
	}

	fm, err := arango.FindMetadataByFilename(qPath, qName, accessKey.BucketId)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("find file failed at /accessKey/files/hidden:",
		//	"File Error")
		return
	}

	if accessKey.BucketId != fm.BucketId {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "permission denied",
		})
		return
	}

	isHidden, err := strconv.ParseBool(qIsHidden)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("parse failed at /accessKey/files/hidden:",
		//	"File Error")
		return
	}
	file, err := arango.ToggleHidden(fm.Path, isHidden)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("toggle failed at /accessKey/files/hidden:",
		//	"File Error")
		return
	}

	c.JSON(http.StatusOK, file)
}
