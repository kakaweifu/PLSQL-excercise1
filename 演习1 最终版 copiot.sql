CREATE OR REPLACE PROCEDURE export_journal_to_csv (
    p_ledger_name   IN VARCHAR2,  -- 元帳名
    p_start_date    IN DATE,      -- 開始日
    p_end_date      IN DATE,      -- 終了日
    p_directory     IN VARCHAR2,  -- 出力ディレクトリ
    p_filename      IN VARCHAR2 DEFAULT NULL  -- 出力ファイル名（省略可能）
) AS
    l_file     UTL_FILE.FILE_TYPE;  -- ファイルハンドル
    l_filename VARCHAR2(255);  -- 実際のファイル名
    l_dir_name VARCHAR2(255) := 'DYNAMIC_DIR_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');  -- 動的ディレクトリ名
    l_line     VARCHAR2(4000);  -- CSV行を格納する変数
    l_count    NUMBER := 0;  -- 処理した行数をカウント

    -- カーソル定義
    CURSOR c_journal_data IS
        WITH DESCRIPTION AS (
            SELECT
                FVVS.VALUE_SET_CODE,  -- 値セットコード
                FVVB.VALUE,           -- 値
                FVVT.DESCRIPTION      -- 説明
            FROM
                FND_VS_VALUES_SETS FVVS
            INNER JOIN
                FND_VS_VALUES_B FVVB ON FVVB.VALUE_SET_ID = FVVS.VALUE_SET_ID
            INNER JOIN
                FND_VS_VALUES_TL FVVT ON FVVT.VALUE_ID = FVVB.VALUE_ID AND FVVT.LANGUAGE = 'JA'
        )
        SELECT
            '"' || TO_CHAR(batches.je_batch_id, 'FM000000000000000') || '","' ||  -- 仕訳バッチID
            TO_CHAR(header.je_header_id, 'FM000000000000000') || '","' ||  -- 仕訳ヘッダID
            REPLACE(ledgers.name, '"', '""') || '","' ||  -- 元帳名
            REPLACE(batches.name, '"', '""') || '","' ||  -- 仕訳バッチ名
            REPLACE(batches.description, '"', '""') || '","' ||  -- 仕訳バッチ摘要
            REPLACE(header.name, '"', '""') || '","' ||  -- 仕訳名
            REPLACE(header.description, '"', '""') || '","' ||  -- 仕訳ヘッダ摘要
            REPLACE(header.period_name, '"', '""') || '","' ||  -- 会計期間
            TO_CHAR(header.default_effective_date, 'YYYY/MM/DD') || '","' ||  -- 会計日
            TO_CHAR(header.posted_date, 'YYYY/MM/DD HH24:MI:SS') || '","' ||  -- 転記日
            REPLACE(src.user_je_source_name, '"', '""') || '","' ||  -- 仕訳ソース
            REPLACE(cat.user_je_category_name, '"', '""') || '","' ||  -- 仕訳カテゴリ
            TO_CHAR(NVL(header.posting_acct_seq_value, 0), 'FM000000000000000') || '","' ||  -- 会計連番
            TO_CHAR(NVL(line.je_line_num, 0), 'FM000000000000000') || '","' ||  -- 明細番号
            REPLACE(line.description, '"', '""') || '","' ||  -- 仕訳明細摘要
            REPLACE(comb.segment1, '"', '""') || '","' ||  -- 会社コード
            REPLACE(D1.DESCRIPTION, '"', '""') || '","' ||  -- 会社摘要
            REPLACE(comb.segment2, '"', '""') || '","' ||  -- LoBコード
            REPLACE(D2.DESCRIPTION, '"', '""') || '","' ||  -- LoB摘要
            REPLACE(comb.segment3, '"', '""') || '","' ||  -- 部門コード
            REPLACE(D3.DESCRIPTION, '"', '""') || '","' ||  -- 部門摘要
            REPLACE(comb.segment4, '"', '""') || '","' ||  -- 勘定科目コード
            REPLACE(D4.DESCRIPTION, '"', '""') || '","' ||  -- 勘定科目摘要
            REPLACE(comb.segment5, '"', '""') || '","' ||  -- Sub Accountコード
            REPLACE(D5.DESCRIPTION, '"', '""') || '","' ||  -- Sub Account摘要
            REPLACE(comb.segment6, '"', '""') || '","' ||  -- 製品コード
            REPLACE(D6.DESCRIPTION, '"', '""') || '","' ||  -- 製品摘要
            REPLACE(comb.segment7, '"', '""') || '","' ||  -- Futureコード
            REPLACE(D7.DESCRIPTION, '"', '""') || '","' ||  -- Future摘要
            REPLACE(comb.segment8, '"', '""') || '","' ||  -- 会社間コード
            REPLACE(D8.DESCRIPTION, '"', '""') || '","' ||  -- 会社間摘要
            REPLACE(line.currency_code, '"', '""') || '","' ||  -- 通貨コード
            TO_CHAR(line.entered_dr, 'FM999,999,999,999,990.00') || '","' ||  -- 入力借方金額
            TO_CHAR(line.entered_cr, 'FM999,999,999,999,990.00') || '","' ||  -- 入力貸方金額
            TO_CHAR(line.accounted_dr, 'FM999,999,999,999,990.00') || '","' ||  -- 計上済借方金額
            TO_CHAR(line.accounted_cr, 'FM999,999,999,999,990.00') || '","' ||  -- 計上済貸方金額
            TO_CHAR(line.currency_conversion_date, 'YYYY/MM/DD') || '","' ||  -- 換算日
            REPLACE(line.currency_conversion_type, '"', '""') || '","' ||  -- 換算レート・タイプ
            REPLACE(line.currency_conversion_rate, '"', '""') || '"' AS CSV  -- 換算レート
        FROM
            GL_JE_BATCHES batches  -- 仕訳バッチ
        INNER JOIN
            GL_JE_HEADERS header ON header.je_batch_id = batches.je_batch_id  -- 仕訳ヘッダ
        INNER JOIN
            GL_LEDGERS ledgers ON ledgers.ledger_id = header.ledger_id  -- 元帳
        INNER JOIN
            GL_JE_SOURCES_TL src ON src.je_source_name = header.je_source AND src.language = 'JA'  -- 仕訳ソース
        INNER JOIN
            GL_JE_CATEGORIES_TL cat ON cat.je_category_name = header.je_category AND cat.language = 'JA'  -- 仕訳カテゴリ
        INNER JOIN
            GL_JE_LINES line ON line.je_header_id = header.je_header_id  -- 仕訳明細
        INNER JOIN
            GL_CODE_COMBINATIONS comb ON comb.code_combination_id = line.code_combination_id  -- コードコンビネーション
        INNER JOIN
            DESCRIPTION D1 ON D1.VALUE_SET_CODE = 'Corporate Company' AND D1.VALUE = comb.segment1  -- 会社コードの説明
        INNER JOIN
            DESCRIPTION D2 ON D2.VALUE_SET_CODE = 'Corporate LoB' AND D2.VALUE = comb.segment2  -- LoBコードの説明
        INNER JOIN
            DESCRIPTION D3 ON D3.VALUE_SET_CODE = 'Japan Cost Center' AND D3.VALUE = comb.segment3  -- 部門コードの説明
        INNER JOIN
            DESCRIPTION D4 ON D4.VALUE_SET_CODE = 'Japan Account' AND D4.VALUE = comb.segment4  -- 勘定科目コードの説明
        INNER JOIN
            DESCRIPTION D5 ON D5.VALUE_SET_CODE = 'Japan Sub Account' AND D5.VALUE = comb.segment5  -- Sub Accountコードの説明
        INNER JOIN
            DESCRIPTION D6 ON D6.VALUE_SET_CODE = 'Corporate Product' AND D6.VALUE = comb.segment6  -- 製品コードの説明
        INNER JOIN
            DESCRIPTION D7 ON D7.VALUE_SET_CODE = 'Japan Future' AND D7.VALUE = comb.segment7  -- Futureコードの説明
        INNER JOIN
            DESCRIPTION D8 ON D8.VALUE_SET_CODE = 'Corporate Company' AND D8.VALUE = comb.segment8  -- 会社間コードの説明
        WHERE
            ledgers.name = p_ledger_name  -- 指定された元帳名
        AND
            TRUNC(header.default_effective_date) BETWEEN p_start_date AND p_end_date  -- 指定された期間内
        ORDER BY
            batches.je_batch_id,  -- 仕訳バッチIDでソート
            header.je_header_id,  -- 仕訳ヘッダIDでソート
            line.je_line_num;  -- 明細番号でソート

    -- ヘッダー行を生成する関数
    FUNCTION get_csv_header RETURN VARCHAR2 IS
    BEGIN
        RETURN '"仕訳バッチID","仕訳ヘッダID","元帳","仕訳バッチ名","仕訳バッチ摘要","仕訳名","仕訳ヘッダ摘要",' ||
               '"会計期間","会計日","転記日","仕訳ソース","仕訳カテゴリ","会計連番","明細番号","仕訳明細摘要",' ||
               '"会社","会社摘要","LoB","LoB摘要","部門","部門摘要","勘定科目","勘定科目摘要","Sub Account",' ||
               '"Sub Account摘要","製品","製品摘要","Future","Future摘要","会社間","会社間摘要","通貨",' ||
               '"入力借方金額","入力貸方金額","計上済借方金額","計上済貸方金額","換算日","換算レート・タイプ","換算レート"';
    END get_csv_header;

    -- エラーログを記録する手続き
    PROCEDURE log_error(p_error_message IN VARCHAR2) IS
        l_log_file UTL_FILE.FILE_TYPE;
    BEGIN
        l_log_file := UTL_FILE.FOPEN(l_dir_name, 'export_journal_error.log', 'A');
        UTL_FILE.PUT_LINE(l_log_file, TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ': ' || p_error_message);
        UTL_FILE.FCLOSE(l_log_file);
    EXCEPTION
        WHEN OTHERS THEN
            -- ログファイルへの書き込みに失敗した場合、標準エラー出力に記録
            DBMS_OUTPUT.PUT_LINE('Error logging failed: ' || SQLERRM);
    END log_error;

BEGIN
    -- 入力パラメータの検証
    IF p_ledger_name IS NULL OR p_start_date IS NULL OR p_end_date IS NULL OR p_directory IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'Required parameters cannot be null');
    END IF;

    IF p_start_date > p_end_date THEN
        RAISE_APPLICATION_ERROR(-20002, 'Start date must be earlier than or equal to end date');
    END IF;

    -- 動的ディレクトリを作成
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY "' || l_dir_name || '" AS ''' || p_directory || '''';

    -- ファイル名が指定されていない場合、デフォルトのファイル名を生成
    IF p_filename IS NULL THEN
        l_filename := 'GL_LINES_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') || '.csv';
    ELSE
        l_filename := p_filename;
    END IF;

    -- ファイルを開く
    l_file := UTL_FILE.FOPEN(l_dir_name, l_filename, 'W', 32767);

    -- ヘッダー行を書き込む
    UTL_FILE.PUT_LINE(l_file, get_csv_header);

    -- データ行を書き込む
    FOR rec IN c_journal_data LOOP
        BEGIN
            UTL_FILE.PUT_LINE(l_file, rec.CSV);
            l_count := l_count + 1;

            -- 1000行ごとにコミット
            IF MOD(l_count, 1000) = 0 THEN
                COMMIT;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                log_error('Error processing row ' || l_count || ': ' || SQLERRM);
        END;
    END LOOP;

    -- 最終コミット
    COMMIT;

    -- ファイルを閉じる
    UTL_FILE.FCLOSE(l_file);

    -- 動的ディレクトリを削除
    EXECUTE IMMEDIATE 'DROP DIRECTORY "' || l_dir_name || '"';

    -- 処理完了メッセージ
    DBMS_OUTPUT.PUT_LINE('Export completed. ' || l_count || ' rows processed.');

EXCEPTION
    WHEN OTHERS THEN
        -- エラーログを記録
        log_error('Unhandled error: ' || SQLERRM);

        -- エラーが発生した場合、ファイルを閉じる
        IF UTL_FILE.IS_OPEN(l_file) THEN
            UTL_FILE.FCLOSE(l_file);
        END IF;

        BEGIN
            EXECUTE IMMEDIATE 'DROP DIRECTORY "' || l_dir_name || '"';
        EXCEPTION
            WHEN OTHERS THEN
                log_error('Failed to drop directory during error handling: ' || SQLERRM);
        END;

        -- エラーを再度発生させる
        RAISE_APPLICATION_ERROR(-20000, 'An error occurred during export: ' || SQLERRM);
END export_journal_to_csv;
/

-- プロシージャの権限を付与（必要に応じて）
GRANT EXECUTE ON export_journal_to_csv TO SCOTT;

-- プロシージャの実行例
BEGIN
    export_journal_to_csv(
        p_ledger_name   => 'Japan Primary Ledger',
        p_start_date    => TO_DATE('2023-05-01', 'YYYY-MM-DD'),
        p_end_date      => TO_DATE('2023-05-31', 'YYYY-MM-DD'),
        p_directory     => 'C:\work\practice1'
    );
END;
/

SHOW ERRORS PROCEDURE export_journal_to_csv;