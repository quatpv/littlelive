-- Calculate outstanding amounts for each document
WITH payments_aggregated AS (
    SELECT
        document_id,
        document_type,
        SUM(amount_paid) AS total_paid
    FROM
        payments
    GROUP BY
        document_id,
        document_type
),
documents_with_payments AS (
    SELECT
        doc.*,
        COALESCE(pay.total_paid, 0) AS total_paid
    FROM
        all_documents doc
        LEFT JOIN payments_aggregated pay ON doc.document_id = pay.document_id
        AND doc.document_type = pay.document_type
)
SELECT
    *,
    (total_amount - total_paid) AS outstanding_amount
FROM
    documents_with_payments
WHERE
    (total_amount - total_paid) > 0