package main

import "fmt"

import "net/http"
import "bytes"
import "encoding/json"
import "io/ioutil"

type ReqDeleteIscsiClient struct {
	ClientIP  string `json:client_ip`
	ClientPid string `json:client_pid`
}

type RespDeleteIscsiClient struct {
	Code int16 `json:"code"`
}

func main() {
	var values ReqDeleteIscsiClient
	values.ClientIP = "10.10.10.10"
	values.ClientPid = "1234"

	//values := map[string]string{"client_ip": "10.32.34.43", "client_pid": "1234"}
	jsonValue, _ := json.Marshal(values)
	resp, err := http.Post("http://10.32.34.43:9300/iscsi/delete", "application/json", bytes.NewBuffer(jsonValue))
	status := resp.Status
	statusCode := resp.StatusCode

	buf, _ := ioutil.ReadAll(resp.Body)

	//buf := new(bytes.Buffer)
	//buf.ReadFrom(resp.Body)
	//body := resp.Body // Body io.ReadCloser

	var res RespDeleteIscsiClient
	//json.Unmarshal(buf.String(), &res)
	json.Unmarshal(buf, &res)
	fmt.Printf("status:%s, code:%d, body:%v, err:%v \n", status, statusCode, res.Code, err)
	fmt.Println("vim-go")
}
