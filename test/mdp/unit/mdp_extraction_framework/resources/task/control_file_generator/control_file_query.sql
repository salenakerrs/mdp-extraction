SELECT COUNT(*), SYSDATETIME(), '{{pos_dt}}' FROM test_tbl2 WHERE dt == {{ pos_dt }}
