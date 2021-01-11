package tec

import (
	"archive/zip"
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

var ROOT_PATH string
var APP_PATH string
var PUBLIC_PATH string
var STATIC_PATH string
var LOG_PATH string
var HOST_NAME string
var CONFIG *Config

// system funcs
func CheckUnix() bool {
	switch runtime.GOOS {
	case "linux", "solaris", "unix", "aix":
		return true
	default:
		return false
	}
}

func GetHostName() string {
	name, err := os.Hostname()
	if err != nil {
		name = "dev.dev"
	}

	var names = strings.Split(name, ".")
	if len(names) < 2 {
		names = []string{"", "dev"}
	} else if strings.Contains(name, "_") {
		names = strings.Split(name, "_")
		if len(names) < 2 {
			names = []string{"", "dev"}
		}
	}

	return names[1]
}

func GetSlashe() string {
	return string(os.PathSeparator)
}

// string funcs
func URLEncode(data string) string {
	return url.QueryEscape(data)
}

func URLDecode(data string) (string, error) {
	return url.QueryUnescape(data)
}

func Base64Encode(data string) string {
	return base64.StdEncoding.EncodeToString([]byte(data))
}

func Base64Decode(data string) string {
	result, _ := base64.StdEncoding.DecodeString(data)
	return string(result)
}

func JsonEncode(data interface{}) string {
	result, _ := json.Marshal(data)
	return string(result)
}

func JsonDecode(data string) map[string]interface{} {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil
	}

	return result
}

func IdEnCode(id int64) int64 {
	sid := (id & 0xff000000)

	sid += (id & 0x0000ff00) << 8
	sid += (id & 0x00ff0000) >> 8
	sid += (id & 0x0000000f) << 4
	sid += (id & 0x000000f0) >> 4

	sid ^= 9416888

	return sid
}

func IdDeCode(sid int64) int64 {
    sid ^= 9416888

    id := (sid & 0xff000000)
    id += (sid & 0x00ff0000) >> 8
    id += (sid & 0x0000ff00) << 8
    id += (sid & 0x000000f0) >> 4
    id += (sid & 0x0000000f) << 4

    return id
}

func Encode(data string) string {
	random := strings.ToUpper(Random(16))
	randNumber := rand.Int63n(12) + 2

	prev := strconv.FormatInt(randNumber, 16) + random[0:randNumber]
	next := strconv.FormatInt(16 - randNumber, 16) + random[randNumber:]

	data = Base64Encode(data)

	data = strings.ReplaceAll(data, "+", "-")
	data = strings.ReplaceAll(data, "/", "_")
	data = strings.ReplaceAll(data, "=", ".")

	return prev + data + next
}

func Decode(data string) string {
	prev := data[0:1]
	randNumber, err := strconv.ParseInt(prev, 16, 32)
	if err != nil || len(data) < int(randNumber) + 16 {
		return ""
	}

	data = data[randNumber + 1:]
	data = data[0:len(data) - 17 + int(randNumber)]

	data = strings.ReplaceAll(data, "-", "+")
	data = strings.ReplaceAll(data, "_", "/")
	data = strings.ReplaceAll(data, ".", "=")

	return Base64Decode(data)
}

func Crc32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}

func Md5(data string) string {
	instance := md5.New()
	instance.Write([]byte(data))

	return hex.EncodeToString(instance.Sum(nil))
}

func Sha1(data string) string {
	instance := sha1.New()
	_, err := io.WriteString(instance, data)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%x", instance.Sum(nil))
}

func Sha1Hmac(source, secret string) string {
	key := []byte(secret)
	instance := hmac.New(sha1.New, key)
	instance.Write([]byte(source))
	signedBytes := instance.Sum(nil)
	signedString := base64.StdEncoding.EncodeToString(signedBytes)
	return signedString
}

func Sha256(data string) string {
	instance := sha256.New()
	instance.Write([]byte(data))
	return fmt.Sprintf("%x", instance.Sum(nil))
}

func Chr(ascii int) string {
	return string(ascii)
}

func Ord(char string) int {
	r, _ := utf8.DecodeRune([]byte(char))
	return int(r)
}

func PKCS7Padding(data []byte, blockSize int) []byte {
	padding := blockSize - len(data) % blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)

	return append(data, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])

	return origData[:(length - unpadding)]
}

func EnCrypt(data string) string {
	var token string
	if CONFIG != nil && CONFIG.App != nil {
		token = CONFIG.App.Token
	}

	token = token + Random(24)
	token = token[0:24]

	block, err := aes.NewCipher([]byte(token))
	if err != nil {
		return ""
	}

	blockSize := block.BlockSize()
	origData := PKCS7Padding([]byte(data), blockSize)
	blockMode := cipher.NewCBCEncrypter(block, []byte(token[:blockSize]))
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)

	return Base64Encode(string(crypted))
}

func DeCrypt(data string) string {
	data = Base64Decode(data)

	var token string
	if CONFIG != nil && CONFIG.App != nil {
		token = CONFIG.App.Token
	}

	token = token + Random(24, false)
	token = token[0:24]

	block, err := aes.NewCipher([]byte(token))
	if err != nil {
		return ""
	}

	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, []byte(token[:blockSize]))
	origData := make([]byte, len(data))
	blockMode.CryptBlocks(origData, []byte(data))
	origData = PKCS7UnPadding(origData)

	return string(origData)
}

func UcFirst(data string) string {
	return strings.ToUpper(data[0:1]) + data[1:]
}

func StripWords(data string) string {
	re, _ := regexp.Compile(`\<[\S\s]+?\>`)
	data = re.ReplaceAllStringFunc(data, strings.ToLower)

	re, _ = regexp.Compile(`\<style[\S\s]+?\</style\>`)
	data = re.ReplaceAllString(data, "")

	re, _ = regexp.Compile(`\<script[\S\s]+?\</script\>`)
	data = re.ReplaceAllString(data, "")

	re, _ = regexp.Compile(`\<[\S\s]+?\>`)
	data = re.ReplaceAllString(data, "")

	re, _ = regexp.Compile(`\s{2,}`)
	data = re.ReplaceAllString(data, "")

	return strings.TrimSpace(data)
}

func StripTags(data string) string {
	reg, _ := regexp.Compile("\\<[\\S\\s]+?\\>")
	return reg.ReplaceAllString(data, "")
}

func CutString(data string, length int, dots ...string) string {
	var dot string
	if len(dots) > 0 {
		dot = dots[0]
	} else {
		dot = ""
	}

	runes := []rune(data)
	if length < 1 || length >= len(runes) + len(dot) {
		return data
	}

	count := 0

	for i := 0; i < len(runes); i++ {
		if unicode.Is(unicode.Han, runes[i]) {
			count += 2
		} else {
			count += 1
		}

		if count > length * 2 - len(dot) {
			return string(runes[0:i]) + dot
		}
	}

	return string(runes[0:length]) + dot
}

func GetUUID() string {
	bytes := make([]byte, 48)
	io.ReadFull(crand.Reader, bytes)

	return Md5(base64.URLEncoding.EncodeToString(bytes))
}

// time funcs
func SubTimer(timer int64) string {
	subtimer := time.Now().Unix() - timer

	if subtimer > 3600 * 216 {
		return time.Unix(timer, 0).Format("01-02")
	} else if subtimer > 3600 * 24 && subtimer < 3600 * 216 {
		return strconv.FormatFloat(float64(subtimer) / (3600 * 24), 'f', 0, 64) + "天前"
	} else if subtimer > 3600 * 12 {
		return "半天前"
	} else if subtimer > 3600 {
		return strconv.FormatFloat(float64(subtimer) / 3600, 'f', 0, 64) + "小时前"
	} else if subtimer > 1800 {
		return "半小时前"
	} else if subtimer > 60 {
		return strconv.FormatFloat(float64(subtimer) / 60, 'f', 0, 64) + "分钟前"
	} else if subtimer > 0 {
		return strconv.FormatInt(subtimer, 10) + "秒前"
	} else if subtimer == 0 {
		return "刚刚"
	} else {
		return ""
	}
}

func Time(args ...int) int64 {
	day := 0
	if len(args) > 0 {
		day = args[0]
	}

	return time.Now().AddDate(0, 0, day).Unix()
}

func Microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func Date(args ...int) int64 {
	day := 0
	if len(args) > 0 {
		day = args[0]
	}

	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, day).Unix()
}

func DateHour(args ...int) int64 {
	day := 0
	if len(args) > 0 {
		day = args[0]
	}

	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.Local).AddDate(0, 0, day).Unix()
}

func DateMinute(args ...int) int64 {
	day := 0
	if len(args) > 0 {
		day = args[0]
	}

	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.Local).AddDate(0, 0, day).Unix()
}

func TimeSpan(timer1 int64, timer2 int64) int {
	return int((timer1 - timer2) / 3600 / 24)
}

// numberic funcs
func Add(num int, args ...int) int {
	for _, arg := range args {
		num += arg
	}

	return num
}

func Subtract(num int, args ...int) int {
	for _, arg := range args {
		num = num - arg
	}

	return num
}

func Multiply(num int, args ...int) int {
	for _, arg := range args {
		num = num * arg
	}

	return num
}

func Divide(num int, args ...int) int {
	for _, arg := range args {
		num = num / arg
	}

	return num
}

func Rand(min, max int) int {
	if min > max {
		return min
	}

	if int31 := 1<<31 - 1; max > int31 {
		return min
	}

	if min == max {
		return min
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(max + 1 - min) + min
}

func Round(value float64) float64 {
	return math.Floor(value + 0.5)
}

func Floor(value float64) float64 {
	return math.Floor(value)
}

func Ceil(value float64) float64 {
	return math.Ceil(value)
}

func Max(nums ...float64) float64 {
	if len(nums) < 2 {
		return nums[0]
	}

	max := nums[0]
	for i := 1; i < len(nums); i++ {
		max = math.Max(max, nums[i])
	}

	return max
}

func Min(nums ...float64) float64 {
	if len(nums) < 2 {
		return nums[0]
	}

	min := nums[0]
	for i := 1; i < len(nums); i++ {
		min = math.Min(min, nums[i])
	}

	return min
}

func Random(length int, args ...bool) string {
	var chars string
	if len(args) > 0 && args[0] {
		chars = "0123456789"
	} else {
		chars = "0123456789abcdefghijklmnopqrstuvwxyz"
	}

	bytes := []byte(chars)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}

	return string(result)
}

func GetRand(rands map[string]int) string {
	result := ""
	randSum := 0
	for _, value := range rands {
		randSum += value
	}

	rand.Seed(time.Now().UnixNano())

	for key, value := range rands {
		randNum := rand.Intn(randSum)
		if randNum <= value {
			result = key
			break
		} else {
			randSum -= value
		}
	}

	return result
}

// type funcs
func InArray(data interface{}, array interface{}) bool {
	switch array.(type) {
	case []int:
		tmps := array.([]int)
		for i := 0; i < len(tmps); i++ {
			if tmps[i] == data.(int) {
				return true
			}
		}
	case []int64:
		tmps := array.([]int64)
		for i := 0; i < len(tmps); i++ {
			if tmps[i] == data.(int64) {
				return true
			}
		}
	case []float64:
		tmps := array.([]float64)
		for i := 0; i < len(tmps); i++ {
			if tmps[i] == data.(float64) {
				return true
			}
		}
	case []string:
		tmps := array.([]string)
		for i := 0; i < len(tmps); i++ {
			if tmps[i] == data.(string) {
				return true
			}
		}
	}

	return false
}

func IndexArray(str string, finds []string) bool {
	for _, find := range finds {
		if strings.Index(str, find) != -1 {
			return true
		}
	}

	return false
}

func IsAnsi(data string) bool {
	result, _ := regexp.Match(`^[\w\_\.]+$`, []byte(data))
	return result
}

func IsCint(data string) bool {
	result, _ := regexp.Match(`^[+-]?[0-9]+$`, []byte(data))
	return result
}

func IsCNumber(data string) bool {
	result, _ := regexp.Match(`^-?\d+(\.\d+)?$`, []byte(data))
	return result
}

func IsEmail(data string) bool {
	result, _ := regexp.Match(`^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$`, []byte(data))
	return result
}

func IsImage(data string) bool {
	return InArray(FileExt(data), []string{"jpg", "jpeg", "gif", "bmp", "png"})
}

func IsIP(data string) bool {
	result, _ := regexp.Match(`^\d+\.\d+\.\d+\.\d+$`, []byte(data))
	return result
}

func IsMobile(data string) bool {
	result, _ := regexp.Match(`^1(0|3|4|5|6|7|8|9)\d{8,9}$`, []byte(data))
	return result
}

func IsRGB(data string) bool {
	result, _ := regexp.Match(`^#[0-9a-fA-F]{6}$`, []byte(data))
	return result
}

func IsPhone(data string) bool {
	result, _ := regexp.Match(`^\d{3}-\d{8}|\d{4}-\d{7}$`, []byte(data))
	return result
}

func IsDateTime(data string) bool {
	result, _ := regexp.Match(`^\d{4}-\d{1,2}-\d{1,2}\s\d{2}:\d{2}$`, []byte(data))
	return result
}

func IsShortdate(data string) bool {
	result, _ := regexp.Match(`^\d{4}-\d{1,2}-\d{1,2}$`, []byte(data))
	return result
}

func IsTimeStamp(data string) bool {
	result, _ := regexp.Match(`^\d{4}-\d{1,2}-\d{1,2}\s\d{2}:\d{2}:\d{2}$`, []byte(data))
	return result
}

func IP2long(ipAddress string) uint32 {
	ip := net.ParseIP(ipAddress)
	if ip == nil {
		return 0
	}

	return binary.BigEndian.Uint32(ip.To4())
}

func Long2ip(properAddress uint32) string {
	ipByte := make([]byte, 4)

	binary.BigEndian.PutUint32(ipByte, properAddress)
	ip := net.IP(ipByte)

	return ip.String()
}

func FormatInt(data string) int {
	num, _ := strconv.Atoi(data)
	return num
}

func FormatBool(data string) bool {
	num, _ := strconv.ParseBool(data)
	return num
}

func FormatInt64(data string) int64 {
	num, _ := strconv.ParseInt(data, 10, 64)
	return num
}

func FormatFloat64(data string) float64 {
	num, _ := strconv.ParseFloat(data, 64)
	return num
}

func FormatBytes(size int64) string {
	units := []string{" B", " KB", " MB", " GB", " TB"}

	tmp := float64(size)
	i := 0

	for ; tmp >= 1024 && i < 4; i++ {
		tmp /= 1024
	}

	return fmt.Sprintf("%.2f", tmp) + units[i]
}

func FormatDiscount(discount float64) string {
	return strconv.FormatFloat(discount, 'f', -1, 64)
}

func FormatMobilePrivacy(mobile string) string {
	if IsMobile(mobile) {
		return mobile[0:3] + "****" + mobile[len(mobile) - 4:]
	} else {
		return mobile
	}
}

func FormatPrice(price float64) string {
	return strconv.FormatFloat(price, 'f', -1, 32)
}

func FormatTime(timer int64, format string) string {
	return time.Unix(timer, 0).Format(format)
}

func FormatTimeToInt64(date string, format string) int64 {
	stamp, _ := time.ParseInLocation(format, date, time.Local)
	return stamp.Unix()
}

// file funs
func Mkdir(filename string, mode os.FileMode) error {
	return os.Mkdir(filename, mode)
}

func RealPath(path string) (string, error) {
	return filepath.Abs(path)
}

func BaseName(path string) string {
	return filepath.Base(path)
}

func Chmod(filename string, mode os.FileMode) bool {
	return os.Chmod(filename, mode) == nil
}

func Chown(filename string, uid, gid int) bool {
	return os.Chown(filename, uid, gid) == nil
}

func IsFile(filename string) bool {
	_, err := os.Stat(filename)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

func IsDir(filename string) bool {
	fd, err := os.Stat(filename)
	if err != nil {
		return false
	}

	fm := fd.Mode()
	return fm.IsDir()
}

func IsReadable(filename string) bool {
	_, err := syscall.Open(filename, syscall.O_RDONLY, 0)
	if err != nil {
		return false
	}

	return true
}

func IsWriteable(filename string) bool {
	_, err := syscall.Open(filename, syscall.O_WRONLY, 0)
	if err != nil {
		return false
	}

	return true
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

func FileExt(file string) string {
	return strings.ToLower(filepath.Ext(file))
}

func FileStat(file string) map[string]interface{} {
	info, err := os.Stat(file)
	if err != nil && os.IsNotExist(err) {
		return nil
	}

	return map[string]interface{}{
		"Name": info.Name(),
		"IsDir": info.IsDir(),
		"ModTime": info.ModTime(),
		"Mode": info.Mode(),
		"Size": info.Size(),
		"Sys": info.Sys(),
	}
}

func FileSize(file string) int64 {
	stat := FileStat(file)
	return stat["Size"].(int64)
}

func FilePutContents(file string, data string, mode os.FileMode) error {
	return ioutil.WriteFile(file, []byte(data), mode)
}

func FileGetContents(file string) string {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return ""
	}

	return string(data)
}

func Unlink(file string) error {
	return os.Remove(file)
}

func UnlinkAll(path string) error {
	return os.RemoveAll(path)
}

func Ini(path string) map[string]map[string]string {
	data := map[string]map[string]string{}

	file, err := os.Open(path)
	if err != nil {
		return nil
	}

	defer file.Close()

	var section string
	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)

		if err != nil {
			if err != io.EOF {
				return nil
			}

			if len(line) == 0 {
				break
			}
		}

		switch {
		case len(line) == 0:
		case string(line[0]) == "#":
		case line[0] == '[':
			section = strings.TrimSpace(line[1 : len(line) - 1])
			if data[section] == nil {
				data[section] = map[string]string{}
			}
		default:
			if strings.IndexAny(line, "=") == -1 {
				continue
			}

			temp := strings.Split(line, "=")

			data[section][strings.TrimSpace(temp[0])] = strings.TrimSpace(temp[1])
		}
	}

	return data
}


var loggerRWMutex sync.RWMutex

func Logger(args ...string) {
	if len(args) == 0 {
		return
	}

	data := args[0]
	dir := ""
	if len(args) > 1 {
		dir = args[1]
	}

	isDir := true
	if len(args) > 2 {
		isDir, _ = strconv.ParseBool(args[2])
	}

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", time.Now().Format("2006-01-02 15:04:05"), "(", Microtime(), ") ", data, "\r\n"))

	go func(data string, path, dir string, isDir bool) {
		loggerRWMutex.Lock()
		defer loggerRWMutex.Unlock()

		now := time.Now()

		var paths = strings.Builder{}

		paths.WriteString(path)
		paths.WriteString("/debug")

		fileName := "/" + now.Format("2006010215") + ".txt"

		if isDir {
			if dir != "" {
				paths.WriteString("/")
				paths.WriteString(dir)
			}

			paths.WriteString("/")
			paths.WriteString(now.Format("200601"))
		} else {
			fileName = "/" + dir + ".txt"
		}

		err := os.MkdirAll(paths.String(), os.ModePerm)
		if err != nil {
			return
		}

		paths.WriteString(fileName)

		file, _ := os.OpenFile(paths.String(), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		_, err = file.WriteString(data)
	}(text.String(), LOG_PATH, dir, isDir)
}

// ext funcs
type Response struct {
	Status int
	Header http.Header
	Body string
	Request *http.Request
	Cookies []*http.Cookie
}

func Http(uri string, params interface{}, header map[string]string, args ...interface{}) *Response {
	method := "GET"
	timeout := 10
	proxyUri := ""

	for _, arg := range args {
		switch arg.(type) {
		case string:
			tmp := strings.ToUpper(arg.(string))
			if tmp == "GET" || tmp == "POST" {
				method = tmp
			} else {
				proxyUri = arg.(string)
			}
		case int:
			timeout, _ = arg.(int)
		}
	}

	var body io.Reader

	if method == "GET" && params != nil {
		switch params.(type) {
		case string:
			body = strings.NewReader(params.(string))
		case map[string]string:
			query := ""

			for key, value := range params.(map[string]string) {
				query += "&" + key + "=" + URLEncode(value)
			}

			if query != "" {
				if !strings.Contains(uri, "?") {
					query = "?" + query[1:]
				}
			}

			uri += query
			body = nil
		}
	} else if method == "POST" && params != nil {
		switch params.(type) {
		case string:
			body = strings.NewReader(params.(string))
		case map[string]string:
			query := ""

			for key, value := range params.(map[string]string) {
				query += "&" + key + "=" + URLEncode(value)
			}

			if query != "" {
				query = query[1:]
			}

			body = strings.NewReader(query)
		}
	}

	client := &http.Client{}
	transport := &http.Transport{}

	if uri[0:5] == "https" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	if proxyUri != "" {
		proxy, err := url.Parse(proxyUri)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxy)
		}
	}

	client.Transport = transport
	client.Timeout = time.Duration(timeout) * time.Second

	request, err := http.NewRequest(method, uri, body)
	if err != nil {
		return &Response{Status:501, Body: err.Error()}
	}

	if header != nil {
		for key, value := range header {
			request.Header[key] = []string{value}
		}
	}

	if CONFIG.App != nil && CONFIG.App.Debug {
		Logger("Http Uri:" + uri + " method:" + method + " header:" + JsonEncode(request.Header), "http")
	}

	response, err := client.Do(request)
	if response == nil || err != nil {
		error := ""
		if err != nil {
			error += "Error:" + err.Error()
		}

		if response != nil {
			error += "Response:" + JsonEncode(response)
		}

		if CONFIG.App != nil && CONFIG.App.Debug {
			Logger("Http Do Error " + error, "http")
		}

		return &Response{Status:502, Body: "Server Error"}
	}

	defer response.Body.Close()

	result := Response {
		Status: response.StatusCode,
		Header: response.Header,
		Request: response.Request,
		Cookies: response.Cookies(),
	}

	if response.StatusCode == 200 {
		body, _ := ioutil.ReadAll(response.Body)
		result.Body = string(body)
	}

	if CONFIG.App != nil && CONFIG.App.Debug {
		Logger("Reponse:" + result.Body, "http")
	}

	return &result
}

func Loop(page int, size int, key int) int {
	return (page - 1) * size + key + 1
}

func Pager(count int64, size int, page int, url string) map[string]interface{} {
	step := 4
	step -= len(strconv.Itoa(page)) - 1
	if step <= 0 {
		step = 1
	}

	offset := int(math.Floor(float64(step) * 0.5))
	pages := int(math.Ceil(float64(count) / float64(size)))

	from := 0
	to := 0

	if step > pages {
		from = 1
		to = pages
	} else {
		from = page - offset
		to = from + step - 1
		if from < 1 {
			to = page + 1 - from
			from = 1

			if to - from < step {
				to = step
			}
		} else if to > pages {
			from = pages - step + 1
			to = pages
		}
	}

	result := map[string]interface{}{}
	result["count"] = count
	result["size"] = size
	result["page"] = page
	result["pages"] = pages
	result["start"] = (page - 1) * size

	if page - offset > 1 && pages > step {
		result["first"] = 1
	}

	if page > 1 {
		result["prev"] = page - 1
	}

	nums := []int{}
	for i := from; i <= to; i++ {
		nums = append(nums, i)
	}

	result["nums"] = nums

	if page < pages {
		result["next"] = page + 1
	}

	if to < pages {
		result["last"] = pages
	}

	anchor := ""
	if strings.Contains(url, "#") {
		paths := strings.Split(url, "#")
		url = paths[0]
		anchor = "#" + paths[1]
	}

	if strings.Contains(url, "?") {
		url += "&amp;"
	} else {
		url += "?"
	}

	result["url"] = url
	result["anchor"] = anchor

	return result
}

func init() {
	ROOT_PATH, _ = os.Getwd()

	if os.Args[0][0] == '/' {
		ROOT_PATH = path.Dir(os.Args[0])
	}

	APP_PATH = ROOT_PATH + "/app"
	PUBLIC_PATH = ROOT_PATH + "/public"
	STATIC_PATH = ROOT_PATH + "/static"
	LOG_PATH = ROOT_PATH + "/logs"

	HOST_NAME = GetHostName()
	if len(os.Args) > 1 {
		if os.Args[1] == "-zip" {
			project := ROOT_PATH[strings.LastIndex(ROOT_PATH, string(byte(os.PathSeparator))) + 1:]
			if len(os.Args) > 2 {
				project = os.Args[2]
			}

			fmt.Println("zip project start...")

			err := ZipDir(ROOT_PATH, ROOT_PATH + string(os.PathSeparator) + project + ".zip", []string{"logs", "pkg", "src", ".idea", "tmp"}, []string{".go", ".mod", ".exe"})
			if err != nil {
				fmt.Println("zip project dir error:", err.Error())
				os.Exit(0)
			}

			fmt.Println("zip file:", fmt.Sprintf("%s%s%s%s", ROOT_PATH, string(os.PathSeparator), project, ".zip"))
			fmt.Println("zip project end")

			os.Exit(0)
		}

		HOST_NAME = os.Args[1]
	}
}

type zipWriteFunc func(info os.FileInfo, file io.Reader, entryName string, fullPath string) error

func zipDirExecute(dir, root, target string, ignorePath, ignoreExtension []string, writerFunc zipWriteFunc) error {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, info := range fileInfos {
		full := filepath.Join(dir, info.Name())

		if info.IsDir() && IndexArray(full, ignorePath) {
			continue
		} else if !info.IsDir() && (full == target || IndexArray(FileExt(full), ignoreExtension)) {
			continue
		}

		var file *os.File
		var reader io.Reader

		if !info.IsDir() {
			file, err = os.Open(full)
			if err != nil {
				return err
			}

			reader = file
		}

		subDir := strings.Replace(dir, root, "", 1)
		if len(subDir) > 0 && subDir[0] == os.PathSeparator {
			subDir = subDir[1:]
		}

		subDir = path.Join(strings.Split(subDir, string(os.PathSeparator))...)
		entryName := path.Join(subDir, info.Name())
		fullPath := path.Join(dir, info.Name())

		if err := writerFunc(info, reader, entryName, fullPath); err != nil {
			if file != nil {
				file.Close()
			}

			return err
		}

		if file != nil {
			if err := file.Close(); err != nil {
				return err
			}
		}

		if info.IsDir() {
			if err := zipDirExecute(full, root, target, ignorePath, ignoreExtension, writerFunc); err != nil {
				return err
			}
		}
	}

	return nil
}

func ZipDir(dir, target string, ignorePath, ignoreExtension []string) error {
	file, err := os.Create(target)
	if err != nil {
		return err
	}

	defer file.Close()

	writer := zip.NewWriter(file)
	defer writer.Close()

	dir = path.Clean(dir)

	return zipDirExecute(dir, dir, target, ignorePath, ignoreExtension, func(info os.FileInfo, file io.Reader, entryName string, fullPath string) error {
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		if !info.IsDir() {
			header.Method = zip.Deflate
		}

		header.Name = entryName

		if info.IsDir() {
			header.Name += "/"
		}

		writer, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}

		if file != nil {
			if _, err := io.Copy(writer, file); err != nil {
				return err
			}
		}

		return nil
	})
}

func Exception(msgs ...string) {
	if err := recover(); err != nil {
		data := ""
		for _, msg := range msgs {
			data += " " + msg
		}

		Logger(data + fmt.Sprint(err), "error")
	}
}