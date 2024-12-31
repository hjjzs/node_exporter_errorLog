package collector

import (
    "bufio"
    "encoding/json"
    "fmt"
    "io"
    "log/slog"
    "os"
    "path/filepath"
    "regexp"
    "runtime"
    "strings"
    "sync"

    "github.com/alecthomas/kingpin/v2"
    "github.com/prometheus/client_golang/prometheus"
)

const (
    errLogSubsystem = "errlog"
    chunkSize       = 1000
)

var (
    errLogPath = kingpin.Flag("collector.errlog.path", "Path to the error log file").Default("/var/log/messages").String()
    keyword    = kingpin.Flag("collector.errlog.keyword", "Regular expression to filter the error log").Default("error").String()
    // 忽略大小写
    ignoreCase = kingpin.Flag("collector.errlog.ignorecase", "Ignore case").Default("true").Bool()
    // 全字匹配
    fullMatch = kingpin.Flag("collector.errlog.fullmatch", "Full match").Default("true").Bool()
    // 服务端口
    port = kingpin.Flag("monitor.port", "monitor port for application").Default("1371").Int()

    stateFile string
)

type errLogCollector struct {
    logger       *slog.Logger
    lastPosition int64
    count        float64
}

type State struct {
    Position int64   `json:"position"`
    Count    float64 `json:"count"`
}

func init() {
    registerCollector(errLogSubsystem, defaultEnabled, NewErrLogCollector)
    // 保存日志读取位置，和错误行数到文件

}

// NewErrLogCollector returns a new Collector exposing memory stats.
func NewErrLogCollector(logger *slog.Logger) (Collector, error) {
    // 设置核心数,使用1/4的CPU核心数
    runtime.GOMAXPROCS(runtime.NumCPU() / 4)

    // 获取日志文件名,只取文件名，不包含路径
    stateFile = "/var/lib/node_exporter/errlog_state_" + filepath.Base(*errLogPath) + ".json"

    file, err := os.Open(*errLogPath)
    if err != nil {
        return nil, fmt.Errorf("无法打开日志文件: %v", err)
    }
    defer file.Close()

    // 获取当前文件尾部位置
    nowPos, err := file.Seek(0, io.SeekEnd)
    if err != nil {
        return nil, fmt.Errorf("无法获取文件位置: %v", err)
    }

    // 读取文件位置和错误行数
    pos, count, err := readPositionAndCount()
    if err != nil {
        return nil, fmt.Errorf("无法读取文件位置和错误行数: %v", err)
    }

    // 如果文件尾部位置小于上次位置，说明文件被截断或日志被清空，从头开始读取
    if nowPos < pos {
        pos = 0
        count = 0
        savePositionAndCount(pos, count)
        logger.Debug("errlog", "warn", "file truncated, reset position and count")
    }

    return &errLogCollector{
        logger:       logger,
        lastPosition: pos,
        count:        count,
    }, nil
}

// 保存日志读取位置，和错误行数到文件
func savePositionAndCount(position int64, count float64) {
    state := State{
        Position: position,
        Count:    count,
    }

    data, err := json.Marshal(state)
    if err != nil {
        return
    }

    // 确保目录存在
    dir := filepath.Dir(stateFile)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return
    }

    // 写入临时文件，防止写入失败
    tmpFile := stateFile + ".tmp"
    if err := os.WriteFile(tmpFile, data, 0644); err != nil {
        return
    }

    // 原子性地重命名文件
    os.Rename(tmpFile, stateFile)
}

// 从文件中读取日志读取位置，和错误行数
func readPositionAndCount() (int64, float64, error) {
    data, err := os.ReadFile(stateFile)
    if err != nil {
        if os.IsNotExist(err) {
            return 0, 0, nil // 文件不存在时返回默认值
        }
        return 0, 0, err
    }

    var state State
    if err := json.Unmarshal(data, &state); err != nil {
        return 0, 0, err
    }

    return state.Position, state.Count, nil
}

type result struct {
    count float64
    err   error
}

// 从日志尾部读取新行，不统计以前的内容，重启后重新统计
func (c *errLogCollector) readNewLines() (float64, error) {
    file, err := os.Open(*errLogPath)
    if err != nil {
        return c.count, fmt.Errorf("无法打开日志文件: %v", err)
    }
    defer file.Close()

    // 检查文件是否被截断
    currentSize, err := file.Seek(0, io.SeekEnd)
    if err != nil {
        return c.count, err
    }

    // 如果文件大小小于上次位置，说明文件被截断或日志被清空，从头开始读取
    if currentSize < c.lastPosition {
        c.lastPosition = 0
        c.count = 0
        savePositionAndCount(c.lastPosition, c.count)
        c.logger.Debug("errlog", "warn", "file truncated, reset position and count")
    }

    // 从上次位置开始读取
    _, err = file.Seek(c.lastPosition, io.SeekStart)
    if err != nil {
        return c.count, err
    }

    /////
    // 编译正则表达式
    flags := ""
    if *ignoreCase {
        flags = "(?i)"
    }
    pattern := *keyword
    if *fullMatch {
        pattern = "\b" + pattern + "\b"
    }
    re, err := regexp.Compile(flags + pattern)
    if err != nil {
        c.logger.Error("invalid regex pattern", "source", "errlog", "error", err.Error())
        return 0, err
    }

    // 创建一个缓冲通道来存储结果
    resultChan := make(chan result)

    // 创建一个工作组来跟踪所有 goroutine
    var wg sync.WaitGroup

    // 创建一个行缓冲切片
    lines := make([]string, 0, chunkSize)

    // 扫描文件并分块处理
    scanner := bufio.NewScanner(file)

    // 启动处理 goroutine
    processChunk := func(chunk []string) {
        defer wg.Done()
        var localCount float64

        for _, line := range chunk {
            if re.MatchString(line) {
                localCount++
            }
        }

        resultChan <- result{count: localCount}
    }

    // 读取文件并分发任务
    for scanner.Scan() {
        lines = append(lines, scanner.Text())

        // 当累积够一定数量的行时，启动一个新的 goroutine 处理
        if len(lines) >= chunkSize {
            wg.Add(1)
            // 创建一个新的切片来存储当前块的内容
            chunk := make([]string, len(lines))
            copy(chunk, lines)
            go processChunk(chunk)

            // 清空切片，为下一块做准备
            lines = lines[:0]
        }
    }

    // 处理剩余的行
    if len(lines) > 0 {
        wg.Add(1)
        go processChunk(lines)
    }

    // 在另一个 goroutine 中等待所有工作完成并关闭结果通道
    go func() {
        wg.Wait()
        close(resultChan)
    }()

    // 收集所有结果
    var totalCount float64
    for r := range resultChan {
        if r.err != nil {
            c.logger.Error("processing chunk", "error", r.err.Error())
            continue
        }
        totalCount += r.count
    }

    if err := scanner.Err(); err != nil {
        c.logger.Error("errlog", "error", err.Error())
        return 0, err
    }

    // 更新文件位置
    c.lastPosition = currentSize

    // 计算匹配行数,如果为空，文件没有变化
    if totalCount == 0 {
        return c.count, nil
    }

    c.count = c.count + totalCount

    // 保存状态
    savePositionAndCount(c.lastPosition, c.count)

    return c.count, nil
}

// 检查服务端口是否存在，使用查看系统文件的方式
// 如果使用命令检查，可能存在运行环境没有安装情况
// 如果使用net库去尝试占用端口的检查方法，如果应用程序在node_exporter检查端口时重启，可能启动失败。
func (c *errLogCollector) checkPort(logger *slog.Logger) float64 {
    portHex := fmt.Sprintf("%04X", *port) // 转换为16进制

    // 检查 TCP4
    tcp4Data, err := os.ReadFile("/proc/net/tcp")
    if err == nil {
        // 按行分割
        lines := strings.Split(string(tcp4Data), "\n")
        for _, line := range lines[1:] { // 跳过标题行
            fields := strings.Fields(line)
            if len(fields) > 2 {
                // local_address 格式为 "IP:PORT"
                localAddr := strings.Split(fields[1], ":")
                if len(localAddr) == 2 && localAddr[1] == portHex {
                    logger.Debug("port check", "status", "in use (TCP4)", "port", *port)
                    return 1
                }
            }
        }
    }

    // 检查 TCP6
    tcp6Data, err := os.ReadFile("/proc/net/tcp6")
    if err == nil {
        lines := strings.Split(string(tcp6Data), "\n")
        for _, line := range lines[1:] {
            fields := strings.Fields(line)
            if len(fields) > 2 {
                localAddr := strings.Split(fields[1], ":")
                if len(localAddr) == 2 && localAddr[1] == portHex {
                    logger.Debug("port check", "status", "in use (TCP6)", "port", *port)
                    return 1
                }
            }
        }
    }

    logger.Debug("port check", "status", "not in use", "port", *port)
    return 0
}

// Update calls (*errLogCollector).getErrLog to get the platform specific
func (c *errLogCollector) Update(ch chan<- prometheus.Metric) error {
    name := "error_keyword_count"

    var metricType = prometheus.GaugeValue

    output, err := c.readNewLines()
    if err != nil {
        c.logger.Error("errlog", "error", err.Error())
        return err
    }

    c.logger.Debug("errlog", "error_keyword_count from:", *errLogPath, "is:", output)
    ch <- prometheus.MustNewConstMetric(
        prometheus.NewDesc(
            prometheus.BuildFQName(namespace, errLogSubsystem, name),
            fmt.Sprintf("Number of errors matching pattern in %s", *errLogPath),
            []string{"keyword", "logpath"},
            nil,
        ),
        metricType, output,
        *keyword, *errLogPath,
    )

    // 添加端口检查指标
    portStatus := c.checkPort(c.logger)
    ch <- prometheus.MustNewConstMetric(
        prometheus.NewDesc(
            prometheus.BuildFQName(namespace, errLogSubsystem, "port_status"),
            fmt.Sprintf("Status of port %d (1: in use, 0: not in use)", *port),
            []string{"port"},
            nil,
        ),
        prometheus.GaugeValue,
        portStatus,
        fmt.Sprintf("%d", *port),
    )

    return nil
}
