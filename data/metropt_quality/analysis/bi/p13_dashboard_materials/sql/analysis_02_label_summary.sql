-- P13 BI semantic SQL for failure-window page.
-- Source artifact: data/metropt_quality/analysis/models/p9_label_summary.tsv
-- Boundary: labels are configured weak labels, not row-level human annotations.

SELECT
  label,
  positive_rows,
  negative_rows,
  positive_rate
FROM p9_label_summary
ORDER BY positive_rate DESC, label;
