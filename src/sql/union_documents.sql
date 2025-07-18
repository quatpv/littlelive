-- Union invoices and credit notes with document type
WITH invoices_with_type AS (
    SELECT
        id AS document_id,
        centre_id,
        class_id,
        student_id,
        invoice_date AS document_date,
        total_amount,
        'invoice' AS document_type
    FROM
        invoices
),
credit_notes_with_type AS (
    SELECT
        id AS document_id,
        centre_id,
        class_id,
        student_id,
        credit_note_date AS document_date,
        total_amount,
        'credit_note' AS document_type
    FROM
        credit_notes
)
SELECT
    *
FROM
    invoices_with_type
UNION ALL
SELECT
    *
FROM
    credit_notes_with_type