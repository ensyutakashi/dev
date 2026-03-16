-- 表題リンク を 表題URL に変更
ALTER TABLE disclosure_info RENAME COLUMN 表題リンク TO 表題_URL;

-- XBRLリンク を XBRLURL に変更
ALTER TABLE disclosure_info RENAME COLUMN XBRLリンク TO XBRL_URL;