package main

import (
	//"encoding/json"
	"github.com/Shopify/sarama"
	//"reflect"
	"strconv"
	//"strings"
	"time"
)

func setUpProducer(host string, port int, mode string) {

	connection := host + ":" + strconv.Itoa(port)

	log.Info("Connecting to Kafka on " + connection + "...")

	config := sarama.NewConfig()
	config.Metadata.Retry.Backoff = (10 * time.Second)

	// We are just streaming metrics, so don't not wait for any Kafka Acks.
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	client, err := sarama.NewClient([]string{connection}, config)
	if err != nil {
		panic(err)
	} else {
		log.Info("Connection to Kafka successful")
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	go pushMetrics(producer, mode)

}

// pushMetrics pushes the load balancer statistic to a Kafka Topic
func pushMetrics(producer sarama.AsyncProducer, mode string) {

	// The list of metrics we want to filter out of the total stats dump from haproxy
	//wantedMetrics := []string{"Scur", "Qcur", "Smax", "Slim", "Weight", "Qtime", "Ctime", "Rtime", "Ttime", "Req_rate", "Req_rate_max", "Req_tot", "Rate", "Rate_lim", "Rate_max"}

	// get metrics every second, for ever.
	for {

		//stats, _ := GetStats("all")
		//localTime := int64(time.Now().Unix())

		// for each proxy in the stats dump, pick out the wanted metrics, parse them and send 'm to Kafka
		//for _, proxy := range stats {

		// filter out the metrics for haproxy's own stats page
		//if proxy.Pxname != "stats" {

		// loop over all wanted metrics for the current proxy
		//for _, metric := range wantedMetrics {

		//fullMetricName := proxy.Pxname + "." + strings.ToLower(proxy.Svname) + "." + strings.ToLower(metric)
		//field := reflect.ValueOf(proxy).FieldByName(metric).String()
		//if field != "" {

		//metricValue, _ := strconv.Atoi(field)

		//metricObj := Metric{fullMetricName, metricValue, localTime}
		//jsonObj, _ := json.MarshalIndent(metricObj, "", " ")

		//ProducerLoop:
		//for {
		//message := &ProducerMessage{Topic: mode + "." + "all" + sarama.StringEncoder("lbmetrics"), Value: sarama.StringEncoder(jsonObj)}
		//select {
		//case producer.Input() <- message:
		//enqueued++
		//case <-signals:
		//producer.AsyncClose() // Trigger a shutdown of the producer.
		//break ProducerLoop
		//}
		//}

		//}
		//}
		//}
		//}
		time.Sleep(3000 * time.Millisecond)
	}
}
