package main

import (
 "net/http"
 "time"
 "github.com/gin-gonic/gin"
)

type Summary struct {
 TotalTx int64   `json:"total_tx"`
 FlaggedTx int64   `json:"flagged_tx"`
 AvgScore float64 `json:"avg_score"`
}

type TimeseriesPoint struct {
 Date string `json:"date"`
 TotalTx int64  `json:"total_tx"`
 FlaggedTx int64  `json:"flagged_tx"`
 AvgScore float64 `json:"avg_score"`
}

type HistogramBin struct {
 Bin string `json:"bin"`
 Cnt int64 `json:"cnt"`
}

type ByChannelRow struct {
 Channel string `json:"channel"`
 TotalTx int64  `json:"total_tx"`
 FlaggedTx int64  `json:"flagged_tx"`
}

type TopTxnRow struct {
 EventDate string  `json:"event_date"`
 TransactionID string `json:"transaction_id"`
 CustomerID string  `json:"customer_id"`
 Amount float64 `json:"amount"`
 Country string  `json:"country"`
 Channel string  `json:"channel"`
 FraudScore float64 `json:"fraud_score"`
}

func main() {
 r := gin.Default()
 r.LoadHTMLGlob("templates/*")
 r.Static("/static", "./static")

 r.GET("/dashboard", func(c *gin.Context) {
  c.HTML(http.StatusOK, "dashboard.html", gin.H{})
 })

 r.GET("/api/summary", func(c *gin.Context) {
  s := Summary{
   TotalTx:   100000,
   FlaggedTx: 2300,
   AvgScore:  0.12,
  }
  c.JSON(http.StatusOK, s)
 })

 r.GET("/api/timeseries", func(c *gin.Context) {
  now := time.Now()
  var res []TimeseriesPoint
  for i := 6; i >= 0; i-- {
   d := now.AddDate(0, 0, -i).Format("2006-01-02")
   res = append(res, TimeseriesPoint{
    Date:      d,
    TotalTx:   10000 + int64(i*100),
    FlaggedTx: 200 + int64(i*10),
    AvgScore:  0.1 + 0.01*float64(i),
   })
  }
  c.JSON(http.StatusOK, res)
 })

 r.GET("/api/score-histogram", func(c *gin.Context) {
  bins := []HistogramBin{
   {"0.0–0.1", 50000},
   {"0.1–0.2", 20000},
   {"0.2–0.3", 10000},
   {"0.3–0.4", 5000},
   {"0.4–0.5", 3000},
   {"0.5–0.6", 1500},
   {"0.6–0.7", 800},
   {"0.7–0.8", 400},
   {"0.8–0.9", 200},
   {"0.9–1.0", 100},
  }
  c.JSON(http.StatusOK, bins)
 })

 r.GET("/api/by-channel", func(c *gin.Context) {
  data := []ByChannelRow{
   {"WEB", 60000, 1200},
   {"MOBILE", 35000, 900},
   {"UNKNOWN", 5000, 200},
  }
  c.JSON(http.StatusOK, data)
 })

 r.GET("/api/top-transactions", func(c *gin.Context) {
  data := []TopTxnRow{
   {"2018-01-01", "T0001", "C123", 999.99, "US", "WEB", 0.98},
   {"2018-01-01", "T0002", "C456", 750.50, "DE", "MOBILE", 0.95},
   {"2018-01-02", "T0003", "C789", 500.00, "FR", "WEB", 0.93},
  }
  c.JSON(http.StatusOK, data)
 })

 r.Run(":8080")
}