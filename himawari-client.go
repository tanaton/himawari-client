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
	"runtime"
	"strconv"
	"time"
)

const (
	ENCODING_NAME   = "himawari-tmp.mp4"
	COMMAND_TIMEOUT = 24 * time.Hour
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

var NumCPU int

func init() {
	n := runtime.NumCPU()
	NumCPU = (n / 4) * 3
	if NumCPU <= 0 {
		NumCPU = 2
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("引数に接続先IPを指定してね")
		os.Exit(1)
	}
	host := os.Args[1]
	for {
		time.Sleep(time.Second)
		t := getTask(host)
		if t == nil {
			continue
		}
		t.preset()
		// エンコード後ファイルが存在しないのでエンコードする
		err := t.ffmpeg(ENCODING_NAME)
		if err != nil {
			fmt.Println(err)
			continue
		}
		err = t.postVideo(host)
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}

func getTask(host string) *Task {
	req, err := http.NewRequest("GET", "http://"+host+"/task", nil)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	req.Header.Set("X-Himawari-Threads", strconv.FormatInt(int64(NumCPU), 10))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Println("仕事が無いみたい")
		return nil
	}
	var t Task
	encerr := json.NewDecoder(res.Body).Decode(&t)
	if encerr != nil {
		fmt.Println(encerr)
		return nil
	}
	if t.Id == "" {
		fmt.Println("UUIDが空になってるよ")
		return nil
	}
	return &t
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

func (t *Task) preset() {
	wfp, err := os.Create(t.PresetName)
	if err != nil {
		panic(err)
	}
	defer wfp.Close()
	wfp.WriteString(t.PresetData)
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
