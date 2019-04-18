package sender

import (
	"log"
	"time"
)

const LIMIT = 200

var MetaDataQueue = NewSafeLinkedList()
var PostPushUrl string
var Debug bool

// 将sender队列中的数据实时发送到transfer，每次最多200条
func StartSender() {
	go startSender()
}

// 将队列中的数据实时发送到transfer，每次最多200条
func startSender() {
	for {
		L := MetaDataQueue.PopBack(LIMIT)
		if len(L) == 0 {
			// 队列为空时，进行sleep
			time.Sleep(time.Millisecond * 200)
			continue
		}

		err := PostPush(L)
		if err != nil {
			log.Println("[E] push to transfer fail", err)
		}
	}
}
