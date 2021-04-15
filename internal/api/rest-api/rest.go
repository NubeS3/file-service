package rest_api

import (
	"github.com/Nubes3/file-service/internal/aggregate"
	"github.com/Nubes3/file-service/internal/api/middlewares"
	"github.com/gin-gonic/gin"
)

func FileRoutes(r *gin.Engine) {

	acr := r.Group("/accessKey/files", middlewares.ApiKeyAuthenticate)
	{
		acr.GET("/all", aggregate.GetAllFileWithAccessKey)

		acr.GET("/hidden/all", aggregate.GetAllFileIncludeHiddenAccessKey)

		acr.POST("/upload", aggregate.UploadFileWithAccessKey)

		acr.GET("/download", aggregate.DownloadFileByIdWithAccessKey)

		acr.GET("/download/*fullpath", aggregate.DownloadFileByPathWithAccessKey)

		acr.POST("/hidden", aggregate.ToggleHiddenByAccessKey)
	}

	ar := r.Group("/auth/files", middlewares.UserAuthenticate)
	{
		ar.GET("/all", aggregate.GetAllFileAuth)

		ar.POST("/upload", aggregate.UploadFileAuth)

		ar.GET("/download", aggregate.DownloadFileByIdAuth)

		ar.GET("/download/*fullpath", aggregate.DownloadFileByPathAuth)

		ar.POST("/hidden", aggregate.ToggleHiddenAuth)
	}

	kpr := r.Group("/signed/files", middlewares.CheckSigned)
	{
		kpr.GET("/all", aggregate.GetAllFileExclueHiddenSigned)

		kpr.GET("/hidden/all", aggregate.GetAllFileSigned)

		kpr.POST("/upload", aggregate.UploadFileSigned)

		kpr.GET("/download", aggregate.DownloadFileByIdSigned)

		kpr.GET("/download/*fullpath", aggregate.DownloadFileByPathSigned)

		kpr.POST("/hidden", aggregate.ToggleHiddenSigned)
	}
}
