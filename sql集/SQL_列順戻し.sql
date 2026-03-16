-- テーブルの列順をTDnet適時開示情報と同じにする
-- テーブル複製
CREATE TABLE disclosure_info_new AS 
SELECT 
    時刻, 
    コード, 会社名, 表題, XBRL, 上場取引所, 更新履歴, 公開日, 連番, 種別, 決算期, 四半期, 
    "ファイル名(連番+公開日+時刻+(種別)+決算月+4Q+コード+会社名+表題)", 
    pdfDL, xbrlDL, 禁則文字, 表題リンク, XBRLリンク
FROM disclosure_info;

--　複製元のテーブル削除
DROP TABLE disclosure_info;

--　複製したテーブルを元の名前に戻す
ALTER TABLE disclosure_info_new RENAME TO disclosure_info;