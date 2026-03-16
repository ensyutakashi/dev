
-- 参考：日付・時刻の変換例
    strptime(公開日, '%Y/%m/%d')::DATE AS 公開日,

----------------------------------------------------------------------

INSERT INTO disclosure_info BY NAME
SELECT
    * EXCLUDE (公開日, 時刻, 決算期, 連番),
    
    -- 直接キャストするだけでOK（strptime不要）
    (公開日 || ' ' || 時刻)::TIMESTAMP AS 時刻,
    公開日::DATE AS 公開日,
    決算期::DATE AS 決算期,
    
    連番::INTEGER AS 連番
    
FROM read_csv_auto(
    '//LS720D7A9/TakashiBK/投資/TDNET/TDNet適時情報開示サービス/UPcsv.csv',
    all_varchar = true,
    encoding = 'UTF-8'
);

