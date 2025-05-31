//go:build testing

package testing

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"
)

const IgniteStartTimeout = "IGNITE_START_TIMEOUT"

type IgniteParams struct {
	InstanceIdx    int
	ClientPort     uint16
	UseAuth        bool
	UseSsl         bool
	UsePersistence bool
	CompactFooter  bool
}

type IgniteInstance interface {
	Kill() error
	Id() int
	ClientPort() uint16
}

type igniteInstanceImpl struct {
	cmd    *exec.Cmd
	params IgniteParams
	doneCh chan interface{}
}

func (ign *igniteInstanceImpl) Id() int {
	return ign.params.InstanceIdx
}

func (ign *igniteInstanceImpl) ClientPort() uint16 {
	return ign.params.ClientPort
}

func (ign *igniteInstanceImpl) Kill() error {
	if ign.doneCh != nil {
		ign.doneCh <- nil
	}
	if ign.cmd == nil || ign.cmd.Process == nil {
		return fmt.Errorf("invalid ign instance: %v", ign)
	}
	pgid, err := syscall.Getpgid(ign.cmd.Process.Pid)
	if err != nil {
		return err
	}
	defer func() {
		clearWorkDirectories()
	}()
	return syscall.Kill(-pgid, syscall.SIGKILL)
}

func getTestDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return path.Dir(filename)
}

func GetIgnitePath() string {
	igniteHome := strings.TrimSpace(os.Getenv("IGNITE_HOME"))
	if len(igniteHome) > 0 {
		return igniteHome
	}
	parentDir := path.Dir(path.Join(getTestDir(), "..", ".."))
	return path.Join(parentDir, "ignite")
}

func GetLogFiles(idx int) ([]string, error) {
	pattern := path.Join(getTestDir(), "logs", fmt.Sprintf("ignite-log-%d*.txt", idx))
	return filepath.Glob(pattern)
}

func clearWorkDirectories() {
	workDir := path.Join(getTestDir(), "work")
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		_ = os.RemoveAll(workDir)
	}
}

func ClearLogs(idx int) {
	if logs, err := GetLogFiles(idx); err == nil {
		for _, log := range logs {
			_ = os.Remove(log)
		}
	}
}

func getIgniteRunner() (string, error) {
	var ext string
	if runtime.GOOS == "windows" {
		ext = ".bat"
	} else {
		ext = ".sh"
	}
	runner := path.Join(GetIgnitePath(), "bin", "ignite"+ext)
	if _, err := os.Stat(runner); err != nil {
		return "", fmt.Errorf("ignite not found, IGNITE_HOME=%s", os.Getenv("IGNITE_HOME"))
	}
	return runner, nil
}

func WithInstanceIndex(idx int) func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.InstanceIdx = idx
	}
}

func WithClientPort(port uint16) func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.ClientPort = port
	}
}

func WithAuth() func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.UseAuth = true
	}
}

func WithPersistence() func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.UsePersistence = true
	}
}

func WithSsl() func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.UseSsl = true
	}
}

func WithCompactFooter(compactFooter bool) func(params *IgniteParams) {
	return func(params *IgniteParams) {
		params.CompactFooter = compactFooter
	}
}

func StartIgnite(opts ...func(params *IgniteParams)) (IgniteInstance, error) {
	params := &IgniteParams{
		InstanceIdx:   0,
		CompactFooter: true,
	}
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(params)
		}
	}
	if params.ClientPort == 0 {
		params.ClientPort = uint16(10800 + params.InstanceIdx)
	}
	ClearLogs(params.InstanceIdx)

	var runner string
	var err error
	if runner, err = getIgniteRunner(); err != nil {
		return nil, err
	}
	if _, err = createLogConfigFile(params); err != nil {
		return nil, err
	}
	var configPath string
	if configPath, err = createConfigFile(params); err != nil {
		return nil, err
	}
	cmd := exec.Command(runner, configPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", "JVM_OPTS",
		fmt.Sprintf("-Djava.net.preferIPv4Stack=true -Xdebug -Xnoagent -Djava.compiler=NONE "+
			"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=%d", 10500+params.InstanceIdx)))
	cmd.Dir = getTestDir()
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start ignite instance: %w\n\toutput:\n:\t%s", err, outBuf.Bytes())
	}
	ignInstance := &igniteInstanceImpl{
		cmd:    cmd,
		params: *params,
		doneCh: make(chan interface{}, 1),
	}
	sigs := make(chan os.Signal, 1024)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigs:
			_ = ignInstance.Kill()
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			var proc *os.Process
			if proc, err = os.FindProcess(syscall.Getpid()); err != nil {
				_ = proc.Signal(sig)
			}
			return
		case <-ignInstance.doneCh:
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			return
		}
	}()
	res := WaitForCondition(func() bool {
		var logFiles []string
		logFiles, err = GetLogFiles(params.InstanceIdx)
		if err != nil {
			panic(fmt.Sprintf("Cannot find log files: %s", err.Error()))
		}
		var reg *regexp.Regexp
		reg, err = regexp.Compile("^Topology snapshot.*")
		if err != nil {
			panic(fmt.Sprintf("Invalid regex: %s", err.Error()))
		}
		res := false
		for _, logFile := range logFiles {
			res, err = MatchLog(reg, logFile)
			if err != nil {
				panic(fmt.Sprintf("Failed to parse file %s, %s", logFile, err.Error()))
			}
			if res {
				break
			}
		}
		return res
	}, waitTimeout())
	if !res {
		ignInstance.doneCh <- nil
		return nil, fmt.Errorf("failed to start ignite instance:\n\toutput:\n:\t%s", outBuf.Bytes())
	}
	return ignInstance, nil
}

func MatchLog(regExp *regexp.Regexp, file string) (bool, error) {
	f, err := os.Open(file)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = f.Close()
	}()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if regExp.MatchString(scanner.Text()) {
			return true, nil
		}
	}
	return false, nil
}

func createLogConfigFile(params *IgniteParams) (string, error) {
	logTpl := path.Join(getTestDir(), "config/log4j.xml.tmpl")
	logPath := path.Join(getTestDir(), fmt.Sprintf("config/log4j-%d.xml", params.InstanceIdx))
	return createTemplate(logPath, logTpl, params)
}

func createConfigFile(params *IgniteParams) (string, error) {
	logTpl := path.Join(getTestDir(), "config/ignite-config.xml.tmpl")
	logPath := path.Join(getTestDir(), fmt.Sprintf("config/ignite-config-%d.xml", params.InstanceIdx))
	return createTemplate(logPath, logTpl, params)
}

func createTemplate(resPath string, tmplPath string, data interface{}) (string, error) {
	tpl, _ := template.ParseFiles(tmplPath)
	f, err := os.Create(resPath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = f.Close()
	}()
	w := bufio.NewWriter(f)
	if err = tpl.Execute(w, data); err != nil {
		return "", err
	}
	if err = w.Flush(); err != nil {
		return "", err
	}
	return resPath, nil
}

func waitTimeout() time.Duration {
	if s := os.Getenv(IgniteStartTimeout); s != "" {
		if i, err := strconv.ParseInt(s, 10, 32); err != nil {
			panic(err)
		} else {
			return time.Duration(i) * time.Second
		}
	}
	return 30 * time.Second
}

func WaitForCondition(condition func() bool, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	doneCh := make(chan bool, 1)
	var cancelFlag atomic.Bool
	go func() {
		defer func() {
			if r := recover(); r != nil {
				doneCh <- false
			}
		}()
		for {
			if res := condition(); res {
				doneCh <- res
				return
			}
			if cancelFlag.Load() {
				doneCh <- false
				return
			}
			runtime.Gosched()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			{
				cancelFlag.Store(true)
			}
		case res := <-doneCh:
			{
				return res
			}
		}
	}
}

func MakeByteArrayPayload(size int) []byte {
	payload := make([]byte, size)
	for i := 0; i < len(payload); i++ {
		payload[i] = byte(i)
	}
	return payload
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func MakeRandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
