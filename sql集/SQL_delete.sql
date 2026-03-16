-- データ削除

-- 連番で削除
DELETE FROM disclosure_info 
WHERE 連番 BETWEEN 41600 AND 41650;

-- 公開日で削除1
DELETE FROM disclosure_info WHERE 公開日 = '2026-01-27';
-- 公開日で削除2
DELETE FROM disclosure_info 
WHERE 公開日 BETWEEN '2026-01-28' AND '2026-01-30';


-- 公開月
DELETE FROM disclosure_info 
WHERE 公開日::VARCHAR LIKE '2026-02%';

-- 時刻
DELETE FROM disclosure_info 
WHERE 時刻 = '2026-01-28 16:00:00';
