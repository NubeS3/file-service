package aggregate

import (
	"github.com/Nubes3/common/models/arangodb"
	"github.com/Nubes3/common/utils"
	arango "github.com/Nubes3/file-service/internal/repo/arangodb"
	"github.com/Nubes3/file-service/internal/repo/nats"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"strconv"
	"time"
)

func GetAllFileAuth(c *gin.Context) {
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
	bid := c.DefaultQuery("bucketId", "")
	if bid == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "missing bid",
		})

		return
	}

	bucket, err := nats.FindBucketById(bid)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "bid invalid",
				})

				return
			}
			if e.ErrType == utils.DbError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "something when wrong",
				})

				//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/all:",
				//	"Db Error")
				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/all:",
		//	"Db Error")
		return
	}

	if uid, ok := c.Get("uid"); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent("uid not found in authenticate at /files/auth/all",
		//	"Unknown Error")
		return
	} else {
		if uid.(string) != bucket.Uid {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "permission denied",
			})
			return
		}
	}

	res, err := arango.FindMetadataByBid(bid, limit, offset, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/all:",
		//	"Db Error")
		return
	}

	c.JSON(http.StatusOK, res)
}

func UploadFileAuth(c *gin.Context) {
	bid := c.DefaultPostForm("bucket_id", "")
	bucket, err := nats.FindBucketById(bid)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "bid invalid",
				})

				return
			}
			if e.ErrType == utils.DbError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "something when wrong",
				})

				//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
				//	"Db Error")
				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
		//	"Db Error")
		return
	}

	if uid, ok := c.Get("uid"); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
		//	"Unknown Error")
		return
	} else {
		if uid.(string) != bucket.Uid {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "permission denied",
			})
			return
		}
	}

	uploadFile, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	queryPath := c.DefaultPostForm("path", "/")
	path := utils.StandardizedPath(bucket.Name+"/"+queryPath, true)

	fileName := c.DefaultPostForm("name", uploadFile.Filename)
	//newPath := bucket.Name + path + fileName

	fileContent, err := uploadFile.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("open file failed at /files/auth/upload:",
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

	res, err := arango.SaveFile(fileContent, bid, path, fileName, isHidden,
		cType, fileSize, time.Duration(ttl))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	//LOG
	//_ = nats.SendUploadFileEvent(res.Id, res.FileId, res.Name, res.Size,
	//	res.BucketId, res.ContentType, res.UploadedDate, res.Path, res.IsHidden)

	c.JSON(http.StatusOK, res)
}

func DownloadFileByIdAuth(c *gin.Context) {
	fid := c.DefaultQuery("fileId", "")
	bid := c.DefaultQuery("bucketId", "")

	bucket, err := nats.FindBucketById(bid)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "bid invalid",
				})

				return
			}
			if e.ErrType == utils.DbError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "something when wrong",
				})

				//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/download",
				//	"Db Error")
				return
			}
		}
	}

	if uid, ok := c.Get("uid"); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent("uid not found at authenticated files/auth/download",
		//	"Unknown Error")
		return
	} else {
		if uid.(string) != bucket.Uid {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "permission denied",
			})
			return
		}
	}

	err = arango.GetFileByFid(fid, func(reader io.Reader, metadata *arangodb.FileMetadata) error {
		if metadata.BucketId != bid {
			return &utils.ModelError{
				Msg:     "invalid bucket",
				ErrType: utils.Invalid,
			}
		}

		extraHeaders := map[string]string{
			"Content-Disposition": `attachment; filename=` + metadata.Name,
		}

		c.DataFromReader(http.StatusOK, metadata.Size, metadata.ContentType, reader, extraHeaders)

		//LOG
		//_ = nats.SendDownloadFileEvent(metadata.Id, metadata.FileId, metadata.Name, metadata.Size,
		//	metadata.BucketId, metadata.ContentType, metadata.UploadedDate, metadata.Path, metadata.IsHidden)

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

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at /files/auth/download:",
		//	"File Error")
		return
	}
}

func DownloadFileByPathAuth(c *gin.Context) {
	fullpath := c.Param("fullpath")
	fullpath = utils.StandardizedPath(fullpath, true)
	bucketName := utils.GetBucketName(fullpath)
	parentPath := utils.GetParentPath(fullpath)
	fileName := utils.GetFileName(fullpath)

	bid := c.DefaultQuery("bucketId", "")
	bucket, err := nats.FindBucketById(bid)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "bid invalid",
				})

				return
			}
			if e.ErrType == utils.DbError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "something when wrong",
				})

				//_ = nats.SendErrorEvent(err.Error()+" at authenticated auth/files/download",
				//	"Db Error")
				return
			}
		}
	}

	if uid, ok := c.Get("uid"); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent("uid not found at authenticated auth/files/download",
		//	"Unknown Error")
		return
	} else {
		if uid.(string) != bucket.Uid {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "permission denied",
			})
			return
		}
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

	err = arango.GetFileByFidIgnoreQueryMetadata(fileMeta.FileId, func(reader io.Reader) error {
		if fileMeta.BucketId != *bucket.Id {
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
		//fileMeta.BucketId, fileMeta.ContentType, fileMeta.UploadedDate, fileMeta.Path, fileMeta.IsHidden)

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

		//_ = nats.SendErrorEvent("download failed: "+err.Error()+" at auth/files/download:",
		//	"File Error")
		return
	}
}

func ToggleHiddenAuth(c *gin.Context) {
	qIsHidden := c.DefaultQuery("hidden", "false")
	qName := c.DefaultQuery("name", "")
	qPath := c.DefaultQuery("path", "")
	qBid := c.DefaultQuery("bucketId", "")

	fm, err := arango.FindMetadataByFilename(qPath, qName, qBid)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("find file failed at auth/files/toggle/hidden:",
		//	"File Error")
		return
	}

	bucket, err := nats.FindBucketById(fm.BucketId)
	if err != nil {
		if e, ok := err.(*utils.ModelError); ok {
			if e.ErrType == utils.NotFound {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "bid invalid",
				})

				return
			}
			if e.ErrType == utils.DbError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "something when wrong",
				})

				//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
				//	"Db Error")
				return
			}
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
		//	"Db Error")
		return
	}

	if uid, ok := c.Get("uid"); !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something when wrong",
		})

		//_ = nats.SendErrorEvent(err.Error()+" at authenticated files/auth/upload:",
		//	"Unknown Error")
		return
	} else {
		if uid.(string) != bucket.Uid {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "permission denied",
			})
			return
		}
	}

	isHidden, err := strconv.ParseBool(qIsHidden)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("parse failed at auth/files/toggle/hidden:",
		//	"File Error")
		return
	}
	file, err := arango.ToggleHidden(fm.Path, isHidden)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "something went wrong",
		})

		//_ = nats.SendErrorEvent("toggle failed at auth/files/toggle/hidden:",
		//	"File Error")
		return
	}

	c.JSON(http.StatusOK, file)
}
