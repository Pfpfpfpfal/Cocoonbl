-- проверка отработки датасета
SELECT 
    dataset, 
    count(*) as total_txn, 
    sum(case when fraud_label=1 then 1 else 0 end) as flagged_txn, 
    avg(fraud_score) as avg_score 
FROM scored_transactions  
group by dataset;