package mr

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestJson(t *testing.T) {
	k:=2
	v:=3
	kv:=fmt.Sprintf("%v:%v",k,v)
	js,err:=json.Marshal(kv)
	if err != nil {
		fmt.Printf("File Key %v Value %v Error: %v\n",  k, v, err)
		panic("Json encode failed")
	}
	var str string
	err =json.Unmarshal(js,&str)
	if err != nil {
		fmt.Printf("File Key %v Value %v Error: %v\n",  k, v, err)
		panic("Json encode failed")
	}
	fmt.Print(str)
}