-- DATE変換
-- 新しい日付型の列を追加
ALTER TABLE disclosure_info ADD COLUMN 決算期_new DATE;
-- TIMESTAMPからDATEへ型を変換してコピー
UPDATE disclosure_info SET 決算期_new = CAST(決算期 AS DATE);
-- 古い列を削除（実行注意！）
ALTER TABLE disclosure_info DROP COLUMN 決算期;
-- 列の名前を元に戻す
ALTER TABLE disclosure_info RENAME COLUMN 決算期_new TO 決算期;



-- VARCHAR変換
ALTER TABLE disclosure_info ADD COLUMN 表題リンク_new VARCHAR;
UPDATE disclosure_info SET 表題リンク_new = CAST(表題リンク AS VARCAR);
ALTER TABLE disclosure_info DROP COLUMN 表題リンク;
ALTER TABLE disclosure_info RENAME COLUMN 表題リンク_new TO 表題リンク;


-- INTEGER変換
ALTER TABLE disclosure_info ADD COLUMN 連番_new INTEGER;
UPDATE disclosure_info SET 連番_new = CAST(連番 AS INTEGER);
ALTER TABLE disclosure_info DROP COLUMN 連番;
ALTER TABLE disclosure_info RENAME COLUMN 連番_new TO 連番;


-- TIMESTAMP変換
-- 新しい一時列を追加
ALTER TABLE disclosure_info ADD COLUMN 時刻_new TIMESTAMP;
-- 日付列と時刻列を合体させて更新
-- ※ '公開日' の部分は実際のテーブルの日付列名に置き換えてください
UPDATE disclosure_info SET 時刻_new = 公開日 + 時刻;
-- 古い列を削除してリネーム
ALTER TABLE disclosure_info DROP COLUMN 時刻;
ALTER TABLE disclosure_info RENAME COLUMN 時刻_new TO 時刻;
