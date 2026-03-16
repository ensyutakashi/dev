SELECT * FROM disclosure_info 
WHERE 連番 = 41651;

～と～の複数
SELECT * FROM disclosure_info 
WHERE 公開日 IN ('2026-01-27', '2026-01-28')
ORDER BY 時刻 DESC;

～から～まで
SELECT * FROM disclosure_info 
WHERE 公開日 BETWEEN '2026-01-27' AND '2026-01-28'
ORDER BY 時刻 DESC;

条件２つ-1
SELECT * FROM disclosure_info 
WHERE 公開日 = '2026-01-28' 
  AND 連番 BETWEEN 41870 AND 41880;

条件２つ-2
select count(*) from disclosure_info where 公開日 between '2026-1-28' and '2026-1-30' and XBRL = 'XBRL'
