-- 「公開日時」という名前でTIMESTAMP型の列を追加
ALTER TABLE disclosure_info ADD COLUMN 公開日時 TIMESTAMP;

-- 公開日(DATE)と時刻(TIME)を足して、新しい列に保存
UPDATE disclosure_info 
SET 公開日時 = CAST(公開日 AS TIMESTAMP) + CAST(時刻 AS INTERVAL);

-- 古い列を削除
ALTER TABLE disclosure_info DROP COLUMN 公開日;
ALTER TABLE disclosure_info DROP COLUMN 時刻;

