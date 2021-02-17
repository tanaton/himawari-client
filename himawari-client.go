package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
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
	base := "/tmp"
	if len(os.Args) >= 3 {
		base = os.Args[2]
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
		os.Kill,
	)
	defer stop()

	// エンコード並列数を決定
	parallel := runtime.NumCPU() / ENCODING_PARALLEL_CORE
	if parallel <= 0 {
		parallel = 1
	}
	syncc := make(chan struct{}, parallel)

	wait := LOOP_WAIT_DEFAULT
	sleep := time.NewTimer(wait)
MAINLOOP:
	for {
		// 並列数を制限
		select {
		case syncc <- struct{}{}:
			log.Debugw("お仕事があるか確認")
		case <-ctx.Done():
			break MAINLOOP
		}
		// お仕事を取得する
		t, err := getTask(ctx, host)
		if err == nil {
			log.Infow("お仕事取得成功",
				"Id", t.Id,
				"Size", t.Size,
				"Name", t.Name,
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
				t.procTask(ctx, host, base)
			}(t)
			// 待ち時間を初期化
			wait = LOOP_WAIT_DEFAULT
		} else {
			log.Infow("お仕事が取得できませんでした", "error", err)
			// 並列数の開放
			<-syncc
		}
		// 待ち時間
		if !sleep.Stop() {
			<-sleep.C
		}
		sleep.Reset(wait)
		select {
		case <-sleep.C:
			wait *= 2
			if wait > LOOP_WAIT_MAX {
				wait = LOOP_WAIT_MAX
			}
		case <-ctx.Done():
			break MAINLOOP
		}
	}
}

func getTask(ctx context.Context, host string) (*Task, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+host+"/task", nil)
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

func (t *Task) procTask(ctx context.Context, host, base string) {
	// プリセットファイルの生成
	ppath, err := t.preset()
	if err != nil {
		log.Warnw("presetの生成に失敗", "error", err, "path", ppath)
		return
	}
	// 作業が終わったらプリセットを消す
	defer os.Remove(ppath)

	ename := filepath.Join(base, t.Id+ENCODING_EXT)
	// エンコード実行
	c, err := t.ffmpeg(ctx, ppath, ename)
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
	err = t.postVideo(ctx, host, ename)
	if err != nil {
		log.Warnw("エンコード後ビデオの転送に失敗", "error", err)
		return
	}
	log.Infow("お仕事完了", "Id", t.Id, "Name", t.Name)
	return
}

func (t *Task) postVideo(ctx context.Context, host, ename string) error {
	pr, pw := io.Pipe()
	w := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer w.Close() // 閉じることでPOSTデータが出来上がる模様
		err := w.WriteField("uuid", t.Id)
		if err != nil {
			log.Warnw("uuidフィールド作成に失敗しました。", "filepath", ename, "error", err)
			return
		}
		_, file := filepath.Split(ename)
		fw, err := w.CreateFormFile("videodata", file)
		if err != nil {
			log.Warnw("パート作成に失敗しました。", "filepath", ename, "error", err)
			return
		}
		rfp, err := os.Open(ename)
		if err != nil {
			log.Warnw("動画ファイルオープンに失敗しました。", "filepath", ename, "error", err)
			return
		}
		defer rfp.Close()
		_, cerr := io.Copy(fw, rfp)
		if cerr != nil {
			log.Warnw("パイプ書き込みに失敗しました。", "filepath", ename, "error", cerr)
			return
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+host+"/task/done", pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
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

func (t *Task) preset() (string, error) {
	wfp, err := os.CreateTemp("", "ffmpeg-preset-")
	if err != nil {
		return "", err
	}
	ppath := wfp.Name()
	_, err = wfp.WriteString(t.PresetData)
	wfp.Close()
	if err != nil {
		os.Remove(ppath)
		return "", err
	}
	return ppath, nil
}

func (t *Task) ffmpeg(ctx context.Context, ppath, outpath string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, COMMAND_TIMEOUT)
	defer cancel()
	if t.Command != "ffmpeg" {
		return "", errors.New("想定していないコマンド")
	}
	args := make([]string, len(t.Args), len(t.Args)+1)
	copy(args, t.Args)
	args = append(args, "-fpre", ppath)
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
