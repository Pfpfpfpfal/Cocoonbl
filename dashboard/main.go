package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/trinodb/trino-go-client/trino"
)

type Summary struct {
	TotalTx int64 `json:"total_tx"`
	FlaggedTx int64 `json:"flagged_tx"`
	AvgScore float64 `json:"avg_score"`
}

type TimeseriesPoint struct {
	Date string `json:"date"`
	TotalTx int64 `json:"total_tx"`
	FlaggedTx int64 `json:"flagged_tx"`
	AvgScore float64 `json:"avg_score"`
}

type HistogramBin struct {
	Bin string `json:"bin"`
	Cnt int64 `json:"cnt"`
}

type ByChannelRow struct {
	Channel string `json:"channel"`
	TotalTx int64 `json:"total_tx"`
	FlaggedTx int64 `json:"flagged_tx"`
}

type TopTxnRow struct {
	EventDate string  `json:"event_date"`
	TransactionID string  `json:"transaction_id"`
	CustomerID string  `json:"customer_id"`
	Amount float64 `json:"amount"`
	Country string `json:"country"`
	Channel string `json:"channel"`
	FraudScore float64 `json:"fraud_score"`
}

var (
    db *sql.DB
    defaultFrom string
    defaultTo string
)

type FraudPointC struct {
    X float64 `json:"x"`
    Y float64 `json:"y"`
    Z float64 `json:"z"`
    Score float64 `json:"score"`
    Label int `json:"label"`
    Cluster int `json:"cluster"`
}

type FraudPoint struct {
    X float64 `json:"x"`
    Y float64 `json:"y"`
    Z float64 `json:"z"`
    Score float64 `json:"score"`
    Label int `json:"label"`
}

type GraphNode struct {
  ID    string  `json:"id"`
  Cat   string  `json:"cat"`
  Value float64 `json:"value"`
}
type GraphEdge struct {
  Source string `json:"source"`
  Target string `json:"target"`
}
type GraphResp struct {
  Nodes []GraphNode `json:"nodes"`
  Edges []GraphEdge `json:"edges"`
}

func initTrinoAndDateRange() *sql.DB {
    dsn := os.Getenv("TRINO_DSN")
    if dsn == "" {
        dsn = "http://admin@localhost:8082?catalog=iceberg&schema=marts"
    }

    db, err := sql.Open("trino", dsn)
    if err != nil {
        log.Fatalf("failed to open trino connection: %v", err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := db.PingContext(ctx); err != nil {
        log.Fatalf("failed to ping trino: %v", err)
    }
    log.Println("Connected to Trino")

    row := db.QueryRowContext(ctx, `
        select
          cast(min(event_date) as varchar),
          cast(max(event_date) as varchar)
        from iceberg.marts.scored_transactions`)

    if err := row.Scan(&defaultFrom, &defaultTo); err != nil {
        log.Fatalf("failed to get default date range: %v", err)
    }
    log.Printf("Default date range: %s .. %s\n", defaultFrom, defaultTo)

    return db
}

func getDateRange(c *gin.Context) (string, string) {
    from := c.Query("from")
    to := c.Query("to")

    if from == "" {
        from = defaultFrom
    }
    if to == "" {
        to = defaultTo
    }
    return from, to
}

func main() {
	db = initTrinoAndDateRange()

	r := gin.Default()
	
	r.Static("/static", "./static")

	r.GET("/dashboard", func(c *gin.Context) {
		c.File("./templates/dashboard.html")
	})

	r.GET("/api/date-range", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"from": defaultFrom,
			"to": defaultTo,
		})
	})

	r.GET("/api/summary", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			count(*) AS total_tx,
			coalesce(
				sum(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END),
				0) AS flagged_tx,
			coalesce(
				avg(fraud_score), 0.0) AS avg_score
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'`,
		from, to,
		)

		var s Summary
		err := db.QueryRowContext(ctx, query).Scan(&s.TotalTx, &s.FlaggedTx, &s.AvgScore)
		if err != nil {
			log.Println("summary query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "summary query failed"})
			return
		}
		c.JSON(http.StatusOK, s)
	})

	r.GET("/api/timeseries", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			CAST(event_date AS VARCHAR) AS d,
			count(*) AS total_tx,
			sum(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS flagged_tx,
			avg(fraud_score) AS avg_score
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
		GROUP BY event_date
		ORDER BY event_date`,
		from, to,
		)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("timeseries query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "timeseries query failed"})
			return
		}
		defer rows.Close()

		var result []TimeseriesPoint
		for rows.Next() {
			var p TimeseriesPoint
			if err := rows.Scan(&p.Date, &p.TotalTx, &p.FlaggedTx, &p.AvgScore); err != nil {
				log.Println("timeseries scan error:", err)
				continue
			}
			result = append(result, p)
		}
		c.JSON(http.StatusOK, result)
	})

	r.GET("/api/score-histogram", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			CAST(floor(fraud_score * 10) / 10.0 AS double) AS bin_from,
			count(*) AS cnt
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
		GROUP BY floor(fraud_score * 10)
		ORDER BY bin_from`,
		from, to,
		)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("hist query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "histogram query failed"})
			return
		}
		defer rows.Close()

		var result []HistogramBin
		for rows.Next() {
			var binFrom float64
			var cnt int64
			if err := rows.Scan(&binFrom, &cnt); err != nil {
				log.Println("hist scan error:", err)
				continue
			}
			label := formatBin(binFrom)
			result = append(result, HistogramBin{Bin: label, Cnt: cnt})
		}
		c.JSON(http.StatusOK, result)
	})

	r.GET("/api/by-channel", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			COALESCE(channel, 'UNKNOWN') AS channel,
			count(*) AS total_tx,
			sum(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS flagged_tx
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
		GROUP BY COALESCE(channel, 'UNKNOWN')
		ORDER BY flagged_tx DESC`,
		from, to,
		)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("by-channel query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "by-channel query failed"})
			return
		}
		defer rows.Close()

		var result []ByChannelRow
		for rows.Next() {
			var rrow ByChannelRow
			if err := rows.Scan(&rrow.Channel, &rrow.TotalTx, &rrow.FlaggedTx); err != nil {
				log.Println("by-channel scan error:", err)
				continue
			}
			result = append(result, rrow)
		}
		c.JSON(http.StatusOK, result)
	})

	r.GET("/api/top-transactions", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			CAST(event_date AS VARCHAR) AS event_date,
			transaction_id,
			customer_id,
			amount,
			COALESCE(country, '') AS country,
			COALESCE(channel, 'UNKNOWN') AS channel,
			fraud_score
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
		ORDER BY fraud_score DESC
		LIMIT 100`,
		from, to,
		)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("top-tx query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "top-transactions query failed"})
			return
		}
		defer rows.Close()

		var result []TopTxnRow
		for rows.Next() {
			var rrow TopTxnRow
			if err := rows.Scan(
				&rrow.EventDate,
				&rrow.TransactionID,
				&rrow.CustomerID,
				&rrow.Amount,
				&rrow.Country,
				&rrow.Channel,
				&rrow.FraudScore,
			); err != nil {
				log.Println("top-tx scan error:", err)
				continue
			}
			result = append(result, rrow)
		}
		c.JSON(http.StatusOK, result)
	})

	r.GET("/api/fraud-cloud-3d", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
			SELECT
			log_amount,
			log10(secs_since_prev_txn + 1)       AS log_secs_prev,
			log10(cust_txn_cnt_7d + 1)           AS log_txn_cnt_7d,
			fraud_score,
			CAST(fraud_label AS integer)
			FROM iceberg.marts.scored_transactions
			WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
			ORDER BY rand()
			LIMIT 3000`,
			from, to,
		)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("fraud-cloud-3d query error:", err)
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		defer rows.Close()

		var pts []FraudPoint
		for rows.Next() {
			var x, y, z, score float64
			var label int
			if err := rows.Scan(&x, &y, &z, &score, &label); err != nil {
				continue
			}
			pts = append(pts, FraudPoint{x, y, z, score, label})
		}

		c.JSON(200, pts)
	})
	
	r.GET("/api/fraud-cloud-3d-cluster", func(c *gin.Context) {
		from, to := getDateRange(c)
		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		SELECT
			c.log_amount,
			c.log_secs_prev,
			c.log_txn_cnt_7d,
			c.fraud_score,
			c.fraud_label,
			c.cluster
		FROM iceberg.marts.scored_transactions_clustered c
		JOIN iceberg.marts.scored_transactions s
			ON c.transaction_id = s.transaction_id
		WHERE s.event_date BETWEEN DATE '%s' AND DATE '%s'
		ORDER BY rand()
		LIMIT 3000`,
		from, to,
	)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("fraud-cloud-3d-cluster query error:", err)
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		defer rows.Close()

		var pts []FraudPointC
		for rows.Next() {
			var x, y, z, score float64
			var label, cluster int
			rows.Scan(&x, &y, &z, &score, &label, &cluster)
			pts = append(pts, FraudPointC{x, y, z, score, label, cluster})
		}

		c.JSON(200, pts)
	})

	r.GET("/api/fraud-links", func(c *gin.Context) {
		from, to := getDateRange(c)
		limitStr := c.DefaultQuery("limit", "200")
		limit, _ := strconv.Atoi(limitStr)
		if limit <= 0 || limit > 1000 { limit = 200 }

		ctx := c.Request.Context()

		query := fmt.Sprintf(`
		WITH top_tx AS (
		SELECT
			CAST(transaction_id AS varchar) AS transaction_id,
			COALESCE(customer_id,'') AS customer_id,
			COALESCE(card_id,'') AS card_id,
			COALESCE(device_id,'') AS device_id,
			COALESCE(email,'') AS email,
			COALESCE(fraud_score, 0.0) AS fraud_score
		FROM iceberg.marts.scored_transactions
		WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
		ORDER BY fraud_score DESC
		LIMIT %d
		)
		SELECT transaction_id, customer_id, card_id, device_id, email, fraud_score
		FROM top_tx
		`, from, to, limit)

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Println("fraud-links query error:", err)
			c.JSON(500, gin.H{"error": "fraud-links query failed"})
			return
		}
		defer rows.Close()

		nodes := make(map[string]GraphNode)
		edges := make([]GraphEdge, 0, limit*4)

		addNode := func(id, cat string, value float64) {
			if id == "" { return }
			if _, ok := nodes[id]; ok { return }
			nodes[id] = GraphNode{ID: id, Cat: cat, Value: value}
		}
		addEdge := func(src, dst string) {
			if src == "" || dst == "" { return }
			edges = append(edges, GraphEdge{Source: src, Target: dst})
		}

		for rows.Next() {
			var txnID, custID, cardID, devID, email string
			var score float64
			if err := rows.Scan(&txnID, &custID, &cardID, &devID, &email, &score); err != nil {
			continue
			}

			txn := "txn:" + txnID
			addNode(txn, "txn", score)

			if custID != "" {
			cust := "cust:" + custID
			addNode(cust, "customer", 1)
			addEdge(txn, cust)
			}
			if cardID != "" {
			card := "card:" + cardID
			addNode(card, "card", 1)
			addEdge(txn, card)
			}
			if devID != "" {
			dev := "dev:" + devID
			addNode(dev, "device", 1)
			addEdge(txn, dev)
			}
			if email != "" {
			mail := "mail:" + email
			addNode(mail, "email", 1)
			addEdge(txn, mail)
			}
		}

		outNodes := make([]GraphNode, 0, len(nodes))
		for _, n := range nodes { outNodes = append(outNodes, n) }

		c.JSON(http.StatusOK, GraphResp{Nodes: outNodes, Edges: edges})
		})

	log.Println("Dashboard listening on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func formatBin(from float64) string {
	to := from + 0.1
	return fmt.Sprintf("%.1f–%.1f", from, to)
}