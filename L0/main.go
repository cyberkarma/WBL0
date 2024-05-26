package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/nats-io/nats.go"
)

var (
	js nats.JetStreamContext
	nc *nats.Conn
	db *pgx.Conn
	mu sync.Mutex
)

// Создание структуры для данных заказа
type Order struct {
	OrderUID    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
	Entry       string `json:"entry"`
	Delivery    struct {
		Name    string `json:"name"`
		Phone   string `json:"phone"`
		Zip     string `json:"zip"`
		City    string `json:"city"`
		Address string `json:"address"`
		Region  string `json:"region"`
		Email   string `json:"email"`
	} `json:"delivery"`
	Payment struct {
		Transaction  string `json:"transaction"`
		RequestID    string `json:"request_id"`
		Currency     string `json:"currency"`
		Provider     string `json:"provider"`
		Amount       int    `json:"amount"`
		PaymentDT    int64  `json:"payment_dt"`
		Bank         string `json:"bank"`
		DeliveryCost int    `json:"delivery_cost"`
		GoodsTotal   int    `json:"goods_total"`
		CustomFee    int    `json:"custom_fee"`
	} `json:"payment"`
	Items []struct {
		ChrtID      int    `json:"chrt_id"`
		TrackNumber string `json:"track_number"`
		Price       int    `json:"price"`
		Rid         string `json:"rid"`
		Name        string `json:"name"`
		Sale        int    `json:"sale"`
		Size        string `json:"size"`
		TotalPrice  int    `json:"total_price"`
		NmID        int    `json:"nm_id"`
		Brand       string `json:"brand"`
		Status      int    `json:"status"`
	} `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

func main() {
	// Подключаемся к серверу NATS
	var err error
	nc, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Создаем JetStream Context
	js, err = nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Создаем поток (если еще не создан)
	streamName := "mystream"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"foo"},
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		log.Fatal(err)
	} else if err == nil {
		fmt.Println("Stream created:", streamName)
	} else {
		fmt.Println("Stream already exists:", streamName)
	}

	// Создаем поток для заказов (если еще не создан)
	orderStreamName := "orders"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     orderStreamName,
		Subjects: []string{"orders"},
	})
	if err != nil && err != nats.ErrStreamNameAlreadyInUse {
		log.Fatal(err)
	} else if err == nil {
		fmt.Println("Order stream created:", orderStreamName)
	} else {
		fmt.Println("Order stream already exists:", orderStreamName)
	}

	// Подключаемся к PostgreSQL
	connStr := "postgres://demo_user:demo_password@localhost:5432/demo_db"
	db, err = pgx.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(context.Background())

	// Проверяем подключение к базе данных
	err = db.Ping(context.Background())
	if err != nil {
		log.Fatal("Unable to connect to database:", err)
	} else {
		fmt.Println("Connected to PostgreSQL database!")
	}

	// Запуск обработчика заказов в горутине
	go processOrders()

	// Используем mux для маршрутизации
	r := mux.NewRouter()
	r.HandleFunc("/publish", publishHandler).Methods("GET")
	r.HandleFunc("/read", readHandler).Methods("GET")
	r.HandleFunc("/order", orderHandler).Methods("POST")
	r.HandleFunc("/order/{order_uid}", getOrderHandler).Methods("GET")
	r.HandleFunc("/orders", getAllOrdersHandler).Methods("GET")

	log.Println("Starting HTTP server on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal(err)
	}
}

// Обработчик для публикации сообщений через GET-запросы
func publishHandler(w http.ResponseWriter, r *http.Request) {
	message := r.URL.Query().Get("message")
	if message == "" {
		http.Error(w, "Message is required", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	// Проверяем и инициализируем JetStream Context, если необходимо
	if js == nil {
		var err error
		js, err = nc.JetStream()
		if err != nil {
			http.Error(w, "Failed to create JetStream context", http.StatusInternalServerError)
			return
		}
	}

	// Публикуем сообщение
	if _, err := js.Publish("foo", []byte(message)); err != nil {
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Published message: %s\n", message)
}

// Обработчик для чтения сообщений через GET-запросы
func readHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	// Проверяем и инициализируем JetStream Context, если необходимо
	if js == nil {
		var err error
		js, err = nc.JetStream()
		if err != nil {
			http.Error(w, "Failed to create JetStream context", http.StatusInternalServerError)
			return
		}
	}

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		http.Error(w, "Failed to subscribe to channel", http.StatusInternalServerError)
		return
	}
	defer sub.Unsubscribe()

	// Ожидаем сообщение в течение 2 секунд
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		http.Error(w, "No messages available", http.StatusGatewayTimeout)
		return
	}

	fmt.Fprintf(w, "Received a message: %s\n", string(msg.Data))
}

// Обработчик для получения JSON и публикации в JetStream
func orderHandler(w http.ResponseWriter, r *http.Request) {
	var order Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if js == nil {
		var err error
		js, err = nc.JetStream()
		if err != nil {
			http.Error(w, "Failed to create JetStream context", http.StatusInternalServerError)
			return
		}
	}

	orderData, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "Failed to serialize order data", http.StatusInternalServerError)
		return
	}

	if _, err := js.Publish("orders", orderData); err != nil {
		http.Error(w, "Failed to publish order data", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Order received: %s\n", order.OrderUID)
}

// Обработчик для чтения из JetStream и записи в базу данных
func processOrders() {
	sub, err := js.SubscribeSync("orders")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	for {
		msg, err := sub.NextMsg(time.Minute)
		if err != nil {
			log.Println("No new messages, waiting...")
			continue
		}

		var order Order
		if err := json.Unmarshal(msg.Data, &order); err != nil {
			log.Println("Failed to deserialize order data:", err)
			continue
		}

		delivery, err := json.Marshal(order.Delivery)
		if err != nil {
			log.Println("Failed to serialize delivery data:", err)
			continue
		}
		payment, err := json.Marshal(order.Payment)
		if err != nil {
			log.Println("Failed to serialize payment data:", err)
			continue
		}
		items, err := json.Marshal(order.Items)
		if err != nil {
			log.Println("Failed to serialize items data:", err)
			continue
		}

		if _, err := db.Exec(context.Background(),
			`INSERT INTO orders (order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (order_uid) DO UPDATE SET
            track_number = EXCLUDED.track_number,
            entry = EXCLUDED.entry,
            delivery = EXCLUDED.delivery,
            payment = EXCLUDED.payment,
            items = EXCLUDED.items,
            locale = EXCLUDED.locale,
            internal_signature = EXCLUDED.internal_signature,
            customer_id = EXCLUDED.customer_id,
            delivery_service = EXCLUDED.delivery_service,
            shardkey = EXCLUDED.shardkey,
            sm_id = EXCLUDED.sm_id,
            date_created = EXCLUDED.date_created,
            oof_shard = EXCLUDED.oof_shard`,
			order.OrderUID, order.TrackNumber, order.Entry, delivery, payment, items,
			order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService, order.Shardkey,
			order.SmID, order.DateCreated, order.OofShard); err != nil {
			log.Println("Failed to insert or update order in database:", err)
		} else {
			log.Println("Order processed:", order.OrderUID)
		}
	}
}

// Новый обработчик для получения заказа по ID
func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderUID := vars["order_uid"]
	if orderUID == "" {
		http.Error(w, "Order ID is required", http.StatusBadRequest)
		return
	}

	var order Order
	var delivery, payment, items []byte
	err := db.QueryRow(context.Background(), `SELECT order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders WHERE order_uid=$1`, orderUID).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &delivery, &payment, &items,
		&order.Locale, &order.InternalSignature, &order.CustomerID, &order.DeliveryService, &order.Shardkey,
		&order.SmID, &order.DateCreated, &order.OofShard)

	if err != nil {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	}

	if err := json.Unmarshal(delivery, &order.Delivery); err != nil {
		http.Error(w, "Failed to deserialize delivery data", http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(payment, &order.Payment); err != nil {
		http.Error(w, "Failed to deserialize payment data", http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(items, &order.Items); err != nil {
		http.Error(w, "Failed to deserialize items data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

// Обработчик для получения всех заказов
func getAllOrdersHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(context.Background(), `SELECT order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders`)
	if err != nil {
		http.Error(w, "Failed to query orders", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var orders []Order

	for rows.Next() {
		var order Order
		var delivery, payment, items []byte
		if err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &delivery, &payment, &items,
			&order.Locale, &order.InternalSignature, &order.CustomerID, &order.DeliveryService, &order.Shardkey,
			&order.SmID, &order.DateCreated, &order.OofShard); err != nil {
			log.Printf("Failed to scan order: %v\n", err)
			http.Error(w, "Failed to scan order", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(delivery, &order.Delivery); err != nil {
			log.Printf("Failed to deserialize delivery data: %v\n", err)
			http.Error(w, "Failed to deserialize delivery data", http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(payment, &order.Payment); err != nil {
			log.Printf("Failed to deserialize payment data: %v\n", err)
			http.Error(w, "Failed to deserialize payment data", http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(items, &order.Items); err != nil {
			log.Printf("Failed to deserialize items data: %v\n", err)
			http.Error(w, "Failed to deserialize items data", http.StatusInternalServerError)
			return
		}

		orders = append(orders, order)
	}

	if rows.Err() != nil {
		log.Printf("Rows error: %v\n", rows.Err())
		http.Error(w, "Error reading orders", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}
