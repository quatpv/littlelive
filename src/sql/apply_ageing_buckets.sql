-- Apply ageing buckets based on document date and as_at_date
WITH documents_with_age AS (
    SELECT
        *,
        DATEDIFF('{as_at_date}', document_date) AS days_outstanding
    FROM
        outstanding_documents
)
SELECT
    centre_id,
    class_id,
    document_id,
    document_date,
    student_id,
    -- Ageing buckets
    CASE
        WHEN days_outstanding >= 0
        AND days_outstanding <= 30 THEN outstanding_amount
        ELSE 0
    END AS day_30,
    CASE
        WHEN days_outstanding >= 31
        AND days_outstanding <= 60 THEN outstanding_amount
        ELSE 0
    END AS day_60,
    CASE
        WHEN days_outstanding >= 61
        AND days_outstanding <= 90 THEN outstanding_amount
        ELSE 0
    END AS day_90,
    CASE
        WHEN days_outstanding >= 91
        AND days_outstanding <= 120 THEN outstanding_amount
        ELSE 0
    END AS day_120,
    CASE
        WHEN days_outstanding >= 121
        AND days_outstanding <= 150 THEN outstanding_amount
        ELSE 0
    END AS day_150,
    CASE
        WHEN days_outstanding >= 151
        AND days_outstanding <= 180 THEN outstanding_amount
        ELSE 0
    END AS day_180,
    CASE
        WHEN days_outstanding > 180 THEN outstanding_amount
        ELSE 0
    END AS day_180_and_above,
    document_type,
    date'{as_at_date}' AS as_at_date
FROM
    documents_with_age
ORDER BY
    centre_id,
    class_id,
    document_id