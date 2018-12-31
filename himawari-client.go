package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	ENCODING_NAME     = "himawari-tmp.mp4"
	COMMAND_TIMEOUT   = 24 * time.Hour
	LOOP_WAIT_DEFAULT = time.Second
	LOOP_WAIT_MAX     = 1000 * time.Second
)

type Task struct {
	Id         string
	Size       int64
	Name       string
	PresetName string
	PresetData string
	Command    string
	Args       []string
}

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.InfoLevel)
}

func main() {
	if len(os.Args) < 2 {
		log.WithFields(log.Fields{
			"len": len(os.Args),
		}).Warn("引数に接続先IPを指定してね")
		os.Exit(1)
	}
	host := os.Args[1]

	wait := LOOP_WAIT_DEFAULT
	for {
		time.Sleep(wait)
		t, err := getTask(host)
		if err != nil {
			log.WithError(err).Info("お仕事が取得できませんでした")
			wait *= 2
			if wait > LOOP_WAIT_MAX {
				wait = LOOP_WAIT_MAX
			}
			continue
		}
		log.WithFields(log.Fields{
			"Id":         t.Id,
			"Size":       t.Size,
			"Name":       t.Name,
			"PresetName": t.PresetName,
			"PresetData": t.PresetData,
			"Command":    t.Command,
			"Args":       t.Args,
		}).Info("お仕事取得成功")
		err = t.preset()
		if err != nil {
			log.WithError(err).Warn("presetの生成に失敗")
			continue
		}
		// エンコード後ファイルが存在しないのでエンコードする
		err = t.ffmpeg(ENCODING_NAME)
		if err != nil {
			log.WithError(err).Warn("ffmpegの実行に失敗")
			continue
		}
		log.WithFields(log.Fields{
			"Id":   t.Id,
			"Name": t.Name,
		}).Info("エンコード成功")
		err = t.postVideo(host)
		if err != nil {
			log.WithError(err).Warn("エンコード後ビデオの転送に失敗")
			continue
		}
		log.WithFields(log.Fields{
			"Id":   t.Id,
			"Name": t.Name,
		}).Info("お仕事完了")
		wait = LOOP_WAIT_DEFAULT
	}
}

func getTask(host string) (*Task, error) {
	req, err := http.NewRequest("GET", "http://"+host+"/task", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Himawari-Threads", "0")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New("仕事が無いみたい")
	}
	var t Task
	encerr := json.NewDecoder(res.Body).Decode(&t)
	if encerr != nil {
		return nil, encerr
	}
	if t.Id == "" {
		return nil, errors.New("UUIDが空になってるよ")
	}
	return &t, nil
}

func (t *Task) postVideo(host string) error {
	tmpfile, err := ioutil.TempFile("", "videodata-")
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name()) // clean up

	sw := NewSizeWriter(tmpfile)
	w := multipart.NewWriter(sw)

	var fw io.Writer
	var ferr error
	{
		ferr = w.WriteField("uuid", t.Id)
		if ferr != nil {
			return ferr
		}
	}
	{
		fw, ferr = w.CreateFormFile("videodata", ENCODING_NAME)
		if ferr != nil {
			return ferr
		}
		rfp, err := os.Open(ENCODING_NAME)
		if err != nil {
			return err
		}
		_, cerr := io.Copy(fw, rfp)
		rfp.Close()
		if cerr != nil {
			return cerr
		}
	}

	w.Close() // 閉じることでPOSTデータが出来上がる模様
	_, err = tmpfile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "http://"+host+"/task/done", tmpfile)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.ContentLength = sw.size
	req.Body = tmpfile
	req.GetBody = func() (io.ReadCloser, error) {
		// 本当はコピーしないといけない
		return tmpfile, nil
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", res.Status)
	}
	return nil
}

func (t *Task) preset() error {
	wfp, err := os.Create(t.PresetName)
	if err != nil {
		return err
	}
	defer wfp.Close()
	n, err := wfp.WriteString(t.PresetData)
	if err != nil {
		return err
	}
	if n != len(t.PresetData) {
		return errors.New("プリセットデータサイズがおかしい")
	}
	return nil
}

func (t *Task) ffmpeg(outpath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), COMMAND_TIMEOUT)
	defer cancel()
	if t.Command != "ffmpeg" {
		return errors.New("想定していないコマンド")
	}
	args := make([]string, len(t.Args), len(t.Args)+1)
	copy(args, t.Args)
	args = append(args, outpath)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type SizeWriter struct {
	size int64
	w    io.Writer
}

func NewSizeWriter(w io.Writer) *SizeWriter {
	return &SizeWriter{
		w: w,
	}
}
func (sw *SizeWriter) Write(b []byte) (int, error) {
	s, err := sw.w.Write(b)
	sw.size += int64(s)
	return s, err
}
