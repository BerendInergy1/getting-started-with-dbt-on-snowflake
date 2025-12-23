-- Test to ensure journal transactions sum correctly
SELECT gje.accountingdate
, SUM(gjae.accountingcurrencyamount) AS accountingcurrencyamount
FROM {{ source('dynamics_365_fo', 'GeneralJournalAccountEntry') }} AS gjae
INNER JOIN {{ source('dynamics_365_fo', 'GeneralJournalEntry') }} AS gje
ON gjae.generaljournalentry = gje.recid
WHERE gje.accountingdate >= DATEADD(DAY, -10, CURRENT_DATE()) --Only test the last 10 days
GROUP BY ALL
HAVING SUM(gjae.accountingcurrencyamount) <> 0
ORDER BY 1

