package api

import (
	"github.com/gin-gonic/gin"
	"github.com/icza/backscanner"
	"github.com/imiskolee/anycdc/pkg/config"
	"net/http"
	"os"
	"path"
	"strings"
)

func LogTailHandler(g *gin.Context) {
	var dto struct {
		FileName string `json:"file"`
		Pos      int64  `json:"pos"`
		Lines    int    `json:"lines"`
	}

	if err := Parse(g, &dto); err != nil {
		return
	}

	fileName := path.Join(config.G.DataDir, dto.FileName)

	lines, newPos, err := readLatestFile(fileName, 0, dto.Pos, dto.Lines)
	if err != nil {
		Error(g, http.StatusBadRequest, "can not read log file "+err.Error())
		return
	}
	Success(g, "logs", map[string]interface{}{
		"content":  strings.Join(lines, "\n"),
		"next_pos": newPos,
	})
}

func readLatestFile(fileName string, totalPos int64, pos int64, lines int) ([]string, int64, error) {

	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, 0, err
	}
	fd, err := os.Open(fileName)
	if err != nil {
		return nil, 0, err
	}

	var startPos int64
	if pos == 0 {
		startPos = fileInfo.Size()
	} else {
		startPos = pos
	}

	scanner := backscanner.New(fd, int(startPos))

	readedLines := 0
	endPos := startPos
	var content []string
	for {
		l, p, err := scanner.Line()
		if err != nil {
			return nil, 0, err
		}
		readedLines++
		content = append(content, l)
		endPos = int64(p)
		if readedLines >= lines {
			break
		}
		if p <= 0 {
			break
		}
	}
	return content, endPos, nil
}
