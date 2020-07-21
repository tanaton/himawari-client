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
	"path/filepath"
	"runtime"
	"time"

	"go.uber.org/zap"
)

const (
	ENCODING_EXT           = ".mp4"
	ENCODING_PARALLEL_CORE = 8
	COMMAND_TIMEOUT        = 24 * time.Hour
	LOOP_WAIT_DEFAULT      = time.Second
	LOOP_WAIT_MAX          = 1000 * time.Second
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

var log *zap.SugaredLogger

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	log = logger.Sugar()
}

func main() {
	if len(os.Args) < 2 {
		log.Warnw("引数に接続先IPを指定してね", "len", len(os.Args))
		os.Exit(1)
	}
	host := os.Args[1]
	base := "."
	if len(os.Args) >= 3 {
		base = os.Args[2]
	}

	// エンコード並列数を決定
	parallel := runtime.NumCPU() / ENCODING_PARALLEL_CORE
	if parallel <= 0 {
		parallel = 1
	}
	syncc := make(chan struct{}, parallel)

	wait := LOOP_WAIT_DEFAULT
	for {
		// 並列数を制限
		syncc <- struct{}{}

		// お仕事を取得する
		t, err := getTask(host)
		if err == nil {
			log.Infow("お仕事取得成功",
				"Id", t.Id,
				"Size", t.Size,
				"Name", t.Name,
				"PresetName", t.PresetName,
				"PresetData", t.PresetData,
				"Command", t.Command,
				"Args", t.Args,
			)
			go func(t *Task) {
				defer func() {
					// 並列数の開放
					<-syncc
				}()
				// お仕事開始
				t.procTask(host, base)
			}(t)
			// 待ち時間を初期化
			wait = LOOP_WAIT_DEFAULT
		} else {
			log.Infow("お仕事が取得できませんでした", "error", err)
			// 並列数の開放
			<-syncc
		}

		// 待ち時間
		time.Sleep(wait)
		wait *= 2
		if wait > LOOP_WAIT_MAX {
			wait = LOOP_WAIT_MAX
		}
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

func (t *Task) procTask(host, base string) {
	// プリセットファイルの生成
	err := t.preset()
	if err != nil {
		log.Warnw("presetの生成に失敗", "error", err)
		return
	}
	// 作業が終わったらプリセットを消す
	defer os.Remove(t.PresetName)

	ename := filepath.Join(base, t.Id+ENCODING_EXT)
	// エンコード実行
	c, err := t.ffmpeg(ename)
	if err != nil {
		log.Warnw("ffmpegの実行に失敗", "error", err, "command", c)
		return
	}
	// 作業が終わったらエンコード済みファイルを消す
	defer os.Remove(ename)
	log.Infow("エンコード成功",
		"Id", t.Id,
		"Name", t.Name,
	)

	// エンコード後ビデオの転送
	err = t.postVideo(host, ename)
	if err != nil {
		log.Warnw("エンコード後ビデオの転送に失敗", "error", err)
		return
	}
	log.Infow("お仕事完了", "Id", t.Id, "Name", t.Name)
	return
}

func (t *Task) postVideo(host, ename string) error {
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
		_, file := filepath.Split(ename)
		fw, ferr = w.CreateFormFile("videodata", file)
		if ferr != nil {
			return ferr
		}
		rfp, err := os.Open(ename)
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
	_, err = wfp.WriteString(t.PresetData)
	wfp.Close()
	if err != nil {
		os.Remove(t.PresetName)
		return err
	}
	return nil
}

func (t *Task) ffmpeg(outpath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), COMMAND_TIMEOUT)
	defer cancel()
	if t.Command != "ffmpeg" {
		return "", errors.New("想定していないコマンド")
	}
	args := make([]string, len(t.Args), len(t.Args)+1)
	copy(args, t.Args)
	args = append(args, outpath)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	return cmd.String(), cmd.Run()
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
