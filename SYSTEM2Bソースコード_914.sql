CREATE OR REPLACE PACKAGE SYSTEM_B_IMPORT_EXPORT AS
 
    -- インポート処理を行う手続き
    PROCEDURE IMPORT(
        I_DIR_NAME IN VARCHAR2,
        I_EMPLOYEES_FILE IN VARCHAR2,
        I_ATTENDANCES_FILE IN VARCHAR2
    );
 
    -- エクスポート処理を行う手続き
    PROCEDURE EXPORT(
        I_DATE_FROM IN DATE,
        I_DATE_TO IN DATE,
        I_DIR_NAME IN VARCHAR2
    );
 
    -- ファイルを開く手続き
    PROCEDURE OPEN_FILE(
        I_FILE_NAME IN VARCHAR2,
        I_OPEN_MODE IN VARCHAR2
    );
 
    -- ファイルを閉じる手続き
    PROCEDURE CLOSE_FILE;
 
    -- ファイルに行を書き込む手続き
    PROCEDURE PRINT_LINE(
        I_BUF IN VARCHAR2
    );
 
    -- 勤怠データをマージする手続き
    PROCEDURE MERGE_ATTENDANCE(
        I_A IN ATTENDANCES%ROWTYPE
    );
 
    -- 複数テーブルのデータをマージする手続き
    PROCEDURE MERGE_MULTI_TABLE_DATA(
        I_ATTENDANCE IN ATTENDANCES%ROWTYPE,
        I_WORK_SCHEDULE IN WORK_SCHEDULES%ROWTYPE,
        I_TIMECARD IN TIMECARDS%ROWTYPE
    );
END SYSTEM_B_IMPORT_EXPORT;
/

CREATE OR REPLACE PACKAGE BODY SYSTEM_B_IMPORT_EXPORT AS
 
    -- 定数定義
    GC_EMPLOYEES_CSV_HEADER   CONSTANT VARCHAR2(64) := '社員ID_A,
    社員ID_B,
    姓,
    名,
    有効フラグ';
    GC_ATTENDANCES_CSV_HEADER CONSTANT VARCHAR2(512) := '社員ID_A,
    社員ID_B,
    日付,
    勤務種別コード,
    出勤予定時刻,
    退勤予定時刻,
    出勤時刻,
    退勤時刻,稼働時間（分）,
    休憩時間（分）,
    遅刻・早退・欠勤時間（分）,
    時間外時間（分）,深夜稼働時間（分）,
    60時間超時間外時間（分）';
    GC_DEBUG                  CONSTANT BOOLEAN := TRUE;
    G_DIR_OBJECT              CHAR(16);
    G_FILE_HANDLE             UTL_FILE.FILE_TYPE;
 
    -- 社員情報を取得するカーソル
    CURSOR G_EMPLOYEES_CUR IS
    SELECT
        RTRIM(EMPLOYEE_ID) AS EMPLOYEE_ID,
        FAMILY_NAME,
        GIVEN_NAME,
        IS_VALID
    FROM
        EMPLOYEES;
 
    -- 勤怠情報を取得するカーソル
    CURSOR G_ATTENDANCES_CUR(
        I_DATE_FROM IN DATE,
        I_DATE_TO IN DATE
    ) IS
    SELECT
        RTRIM(A.EMPLOYEE_ID)          AS EMPLOYEE_ID,
        A.ATTENDANCE_DATE,
        RTRIM(A.ATTENDANCE_TYPE_CODE) AS ATTENDANCE_TYPE_CODE,
        WS.SCHEDULED_START_TIME,
        WS.SCHEDULED_END_TIME,
        TC.START_TIME,
        TC.END_TIME,
        A.WORKING_MINUTES,
        WS.BREAKING_MINUTES,
        A.ABSENCE_MINUTES,
        A.OVER_TIME_MINUTES,
        A.MIDNIGHT_WORKING_MINUTES,
        A.OVER_60_OVER_TIME_MINUTES
    FROM
        ATTENDANCES    A
        LEFT JOIN WORK_SCHEDULES WS
        ON A.EMPLOYEE_ID = WS.EMPLOYEE_ID
        AND A.ATTENDANCE_DATE = WS.ATTENDANCE_DATE
        LEFT JOIN TIMECARDS TC
        ON A.EMPLOYEE_ID = TC.EMPLOYEE_ID
        AND A.ATTENDANCE_DATE = TC.ATTENDANCE_DATE
    WHERE
        A.ATTENDANCE_DATE BETWEEN I_DATE_FROM AND I_DATE_TO;
 
    -- デバッグ情報を出力する手続き
    PROCEDURE DEBUG_PRINT(
        I_MSG IN VARCHAR2
    ) IS
    BEGIN
        IF GC_DEBUG THEN
            DBMS_OUTPUT.PUT_LINE('デバッグ: '
                                 || I_MSG);
        END IF;
    END DEBUG_PRINT;
 

    -- ディレクトリオブジェクトを作成する手続き
    PROCEDURE CREATE_DIRECTORY(
        I_DIR_NAME IN VARCHAR2
    ) IS
    BEGIN
        LOOP
            BEGIN
                G_DIR_OBJECT := DBMS_RANDOM.STRING('U', 16);
                EXECUTE IMMEDIATE 'CREATE DIRECTORY '
                                  || G_DIR_OBJECT
                                  || ' AS '''
                                  || I_DIR_NAME
                                  || '''';
                DEBUG_PRINT('ディレクトリ作成: '
                            || G_DIR_OBJECT
                            || ' ('
                            || I_DIR_NAME
                            || ')');
                EXIT;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE = -955 THEN
                        CONTINUE;
                    ELSE
                        RAISE;
                    END IF;
            END;
        END LOOP;
    END CREATE_DIRECTORY;
 

    -- ディレクトリオブジェクトを削除する手続き
    PROCEDURE DROP_DIRECTORY IS
    BEGIN
        EXECUTE IMMEDIATE 'DROP DIRECTORY '
                          || G_DIR_OBJECT;
        DEBUG_PRINT('ディレクトリ削除: '
                    || G_DIR_OBJECT);
    END DROP_DIRECTORY;
 

    -- ファイル名を生成する関数
    FUNCTION CREATE_FILE_NAME(
        P_PREFIX IN VARCHAR2
    ) RETURN VARCHAR2 IS
    BEGIN
        RETURN P_PREFIX
               || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MI')
                  || '.csv';
    END CREATE_FILE_NAME;
 

    -- ファイルを開く手続き
    PROCEDURE OPEN_FILE(
        I_FILE_NAME IN VARCHAR2,
        I_OPEN_MODE IN VARCHAR2
    ) IS
    BEGIN
        G_FILE_HANDLE := UTL_FILE.FOPEN_NCHAR(G_DIR_OBJECT, I_FILE_NAME, I_OPEN_MODE);
        DEBUG_PRINT('ファイルオープン: '
                    || I_FILE_NAME);
    END OPEN_FILE;
 

    -- ファイルを閉じる手続き
    PROCEDURE CLOSE_FILE IS
    BEGIN
        IF UTL_FILE.IS_OPEN(G_FILE_HANDLE) THEN
            UTL_FILE.FCLOSE(G_FILE_HANDLE);
        END IF;
    END CLOSE_FILE;
 

    -- ファイルに行を書き込む手続き
    PROCEDURE PRINT_LINE(
        I_BUF IN VARCHAR2
    ) IS
    BEGIN
        UTL_FILE.PUT_LINE_NCHAR(G_FILE_HANDLE, RTRIM(I_BUF));
    END PRINT_LINE;
 

    -- ファイルから行を読み込む関数
    FUNCTION GET_LINE RETURN VARCHAR2 IS
        L_BUF NVARCHAR2(4098);
    BEGIN
        UTL_FILE.GET_LINE_NCHAR(G_FILE_HANDLE, L_BUF);
        L_BUF := RTRIM(L_BUF, CHR(13)
                              || CHR(10));
        DEBUG_PRINT(L_BUF);
        RETURN TO_CHAR(L_BUF);
    END GET_LINE;
 

    -- CSVの項目を取得する関数
    FUNCTION GET_CSV_ITEM(
        I_CSV IN VARCHAR2,
        IO_START_POS IN OUT PLS_INTEGER
    ) RETURN VARCHAR2 IS
        L_END_POS PLS_INTEGER;
        L_ITEM    VARCHAR2(1024);
    BEGIN
        L_END_POS := INSTR(I_CSV, ',', IO_START_POS);
        IF L_END_POS > 0 THEN
            L_ITEM := SUBSTR(I_CSV, IO_START_POS, L_END_POS - IO_START_POS);
            IO_START_POS := L_END_POS + 1;
        ELSE
            L_ITEM := SUBSTR(I_CSV, IO_START_POS);
            IO_START_POS := 0;
        END IF;

        RETURN L_ITEM;
    END GET_CSV_ITEM;
 

    -- 社員IDを変換する関数
    FUNCTION CONVERT_EMPLOYEE_ID(
        P_EMPLOYEE_ID_A IN VARCHAR2
    ) RETURN VARCHAR2 IS
        V_PADDED_ID   VARCHAR2(6);
        V_SUM         NUMBER := 0;
        V_DIGIT       NUMBER;
        V_CHECK_DIGIT NUMBER;
        V_TRIMMED_ID  VARCHAR2(20);
    BEGIN
        V_TRIMMED_ID := TRIM(P_EMPLOYEE_ID_A);
        IF SUBSTR(V_TRIMMED_ID, 1, 1) = 'H' THEN
            RETURN V_TRIMMED_ID;
        END IF;
 

        -- 社員IDの形式を検証
        IF NOT REGEXP_LIKE(V_TRIMMED_ID, '^[0-9]+$') THEN
            RAISE_APPLICATION_ERROR(-20006, '無効な社員ID形式: '
                                            || V_TRIMMED_ID);
        END IF;
 

        -- チェックディジットの計算
        V_PADDED_ID := LPAD(V_TRIMMED_ID, 6, '0');
        FOR I IN 1..6 LOOP
            V_DIGIT := TO_NUMBER(SUBSTR(V_PADDED_ID, I, 1));
            IF MOD(I, 2) = 1 THEN
                V_DIGIT := V_DIGIT * 2;
                IF V_DIGIT > 9 THEN
                    V_DIGIT := V_DIGIT - 9;
                END IF;
            END IF;

            V_SUM := V_SUM + V_DIGIT;
        END LOOP;

        V_CHECK_DIGIT := MOD(10 - MOD(V_SUM, 10), 10);
        RETURN 'H'
               || V_PADDED_ID
               || V_CHECK_DIGIT;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20007, '社員ID変換エラー: '
                                            || SQLERRM
                                            || ' (ID: '
                                            || P_EMPLOYEE_ID_A
                                            || ')');
    END CONVERT_EMPLOYEE_ID;
 

    -- CSVデータを社員レコードに変換する関数
    FUNCTION TO_EMPLOYEE(
        I_CSV IN VARCHAR2
    ) RETURN EMPLOYEES%ROWTYPE IS
        L_EMPLOYEE  EMPLOYEES%ROWTYPE;
        L_START_POS PLS_INTEGER := 1;
        L_DUMMY     VARCHAR(8);
    BEGIN
        L_EMPLOYEE.EMPLOYEE_ID := TO_NUMBER(GET_CSV_ITEM(I_CSV, L_START_POS));
        L_DUMMY := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_EMPLOYEE.FAMILY_NAME := REPLACE(GET_CSV_ITEM(I_CSV, L_START_POS), CHR(9), ',');
        L_EMPLOYEE.GIVEN_NAME := REPLACE(GET_CSV_ITEM(I_CSV, L_START_POS), CHR(9), ',');
        L_EMPLOYEE.IS_VALID := TO_NUMBER(GET_CSV_ITEM(I_CSV, L_START_POS));
        RETURN L_EMPLOYEE;
    END TO_EMPLOYEE;
 

    -- 社員データをマージする手続き
    PROCEDURE MERGE_EMPLOYEE(
        I_E IN EMPLOYEES%ROWTYPE
    ) IS
    BEGIN
 
        -- 既存の社員データを更新または新規社員データを挿入
        MERGE INTO EMPLOYEES USING DUAL ON (EMPLOYEE_ID = I_E.EMPLOYEE_ID) WHEN MATCHED THEN UPDATE SET FAMILY_NAME = I_E.FAMILY_NAME, GIVEN_NAME = I_E.GIVEN_NAME, IS_VALID = I_E.IS_VALID, LAST_UPDATE_DATE = SYSTIMESTAMP WHEN NOT MATCHED THEN INSERT ( EMPLOYEE_ID, FAMILY_NAME, GIVEN_NAME, IS_VALID, CREATION_DATE, LAST_UPDATE_DATE ) VALUES ( I_E.EMPLOYEE_ID, I_E.FAMILY_NAME, I_E.GIVEN_NAME, I_E.IS_VALID, SYSTIMESTAMP, SYSTIMESTAMP );
    END MERGE_EMPLOYEE;
 

    -- CSVデータを勤怠レコードに変換する関数
    FUNCTION TO_ATTENDANCE(
        I_CSV IN VARCHAR2
    ) RETURN ATTENDANCES%ROWTYPE IS
        L_ATTENDANCE    ATTENDANCES%ROWTYPE;
        L_WORK_SCHEDULE WORK_SCHEDULES%ROWTYPE;
        L_TIMECARD      TIMECARDS%ROWTYPE;
        L_START_POS     PLS_INTEGER := 1;
        L_DUMMY         VARCHAR2(8);
        L_DATE_STR      VARCHAR2(20);
        L_TIME_STR      VARCHAR2(10);
    BEGIN
        L_ATTENDANCE.EMPLOYEE_ID := GET_CSV_ITEM(I_CSV, L_START_POS);
 
        -- 社員IDが空の場合はこのレコードをスキップ
        IF L_ATTENDANCE.EMPLOYEE_ID IS NULL OR L_ATTENDANCE.EMPLOYEE_ID = '' THEN
            RETURN NULL;
        END IF;

        L_DUMMY := GET_CSV_ITEM(I_CSV, L_START_POS); -- EMPLOYEE_ID_Bをスキップ
        L_DATE_STR := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_ATTENDANCE.ATTENDANCE_DATE := TO_DATE(L_DATE_STR, 'YYYY/MM/DD');
        L_ATTENDANCE.ATTENDANCE_TYPE_CODE := GET_CSV_ITEM(I_CSV, L_START_POS);
 
        -- WORK_SCHEDULESデータの取得
        L_TIME_STR := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_WORK_SCHEDULE.SCHEDULED_START_TIME := L_TIME_STR;
        L_TIME_STR := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_WORK_SCHEDULE.SCHEDULED_END_TIME := L_TIME_STR;
 
        -- TIMECARDSデータの取得
        L_TIME_STR := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_TIMECARD.START_TIME := L_TIME_STR;
        L_TIME_STR := GET_CSV_ITEM(I_CSV, L_START_POS);
        L_TIMECARD.END_TIME := L_TIME_STR;
 
        -- ATTENDANCESデータの取得
        L_ATTENDANCE.WORKING_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
        L_WORK_SCHEDULE.BREAKING_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
        L_ATTENDANCE.ABSENCE_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
        L_ATTENDANCE.OVER_TIME_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
        L_ATTENDANCE.MIDNIGHT_WORKING_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
        L_ATTENDANCE.OVER_60_OVER_TIME_MINUTES := TO_NUMBER(NVL(GET_CSV_ITEM(I_CSV, L_START_POS), '0'));
 
        -- 複数テーブルのデータを処理する新しい手続きを呼び出し
        MERGE_MULTI_TABLE_DATA(L_ATTENDANCE, L_WORK_SCHEDULE, L_TIMECARD);
        RETURN L_ATTENDANCE;
    EXCEPTION
        WHEN OTHERS THEN
            DEBUG_PRINT('TO_ATTENDANCEでエラー発生: '
                        || SQLERRM);
            RAISE_APPLICATION_ERROR(-20005, '勤怠データの解析中にエラーが発生しました: '
                                            || SQLERRM
                                            || ' (行: '
                                            || I_CSV
                                            || ')');
    END TO_ATTENDANCE;
 

    -- 勤怠データをマージする手続き
    PROCEDURE MERGE_ATTENDANCE(
        I_A IN ATTENDANCES%ROWTYPE
    ) IS
    BEGIN
 
        -- 既存の勤怠データを更新または新規勤怠データを挿入
        MERGE INTO ATTENDANCES A USING (
            SELECT
                I_A.EMPLOYEE_ID                 AS EMPLOYEE_ID,
                I_A.ATTENDANCE_DATE             AS ATTENDANCE_DATE
            FROM
                DUAL
        ) I ON (A.EMPLOYEE_ID = I.EMPLOYEE_ID
        AND A.ATTENDANCE_DATE = I.ATTENDANCE_DATE) WHEN MATCHED THEN UPDATE SET A.ATTENDANCE_TYPE_CODE = I_A.ATTENDANCE_TYPE_CODE, A.WORKING_MINUTES = I_A.WORKING_MINUTES, A.ABSENCE_MINUTES = I_A.ABSENCE_MINUTES, A.OVER_TIME_MINUTES = I_A.OVER_TIME_MINUTES, A.MIDNIGHT_WORKING_MINUTES = I_A.MIDNIGHT_WORKING_MINUTES, A.OVER_60_OVER_TIME_MINUTES = I_A.OVER_60_OVER_TIME_MINUTES, A.LAST_UPDATE_DATE = SYSTIMESTAMP WHEN NOT MATCHED THEN INSERT (EMPLOYEE_ID, ATTENDANCE_DATE, ATTENDANCE_TYPE_CODE, WORKING_MINUTES, ABSENCE_MINUTES, OVER_TIME_MINUTES, MIDNIGHT_WORKING_MINUTES, OVER_60_OVER_TIME_MINUTES, CREATION_DATE, LAST_UPDATE_DATE) VALUES (I_A.EMPLOYEE_ID, I_A.ATTENDANCE_DATE, I_A.ATTENDANCE_TYPE_CODE, I_A.WORKING_MINUTES, I_A.ABSENCE_MINUTES, I_A.OVER_TIME_MINUTES, I_A.MIDNIGHT_WORKING_MINUTES, I_A.OVER_60_OVER_TIME_MINUTES, SYSTIMESTAMP, SYSTIMESTAMP);
    END MERGE_ATTENDANCE;
 

    -- 複数テーブルのデータをマージする手続き
    PROCEDURE MERGE_MULTI_TABLE_DATA(
        I_ATTENDANCE IN ATTENDANCES%ROWTYPE,
        I_WORK_SCHEDULE IN WORK_SCHEDULES%ROWTYPE,
        I_TIMECARD IN TIMECARDS%ROWTYPE
    ) IS
    BEGIN
 
        -- 社員IDが空の場合は処理をスキップ
        IF I_ATTENDANCE.EMPLOYEE_ID IS NULL OR I_ATTENDANCE.EMPLOYEE_ID = '' THEN
            RETURN;
        END IF;
 

        -- ATTENDANCESテーブルの更新
        MERGE_ATTENDANCE(I_ATTENDANCE);
 
        -- WORK_SCHEDULESテーブルの更新
        IF I_WORK_SCHEDULE.EMPLOYEE_ID IS NOT NULL AND I_WORK_SCHEDULE.ATTENDANCE_DATE IS NOT NULL THEN
            MERGE INTO WORK_SCHEDULES WS USING (
                SELECT
                    I_WORK_SCHEDULE.EMPLOYEE_ID     AS EMPLOYEE_ID,
                    I_WORK_SCHEDULE.ATTENDANCE_DATE AS ATTENDANCE_DATE
                FROM
                    DUAL
            ) I ON (WS.EMPLOYEE_ID = I.EMPLOYEE_ID
            AND WS.ATTENDANCE_DATE = I.ATTENDANCE_DATE) WHEN MATCHED THEN
                UPDATE
                SET
                    WS.SCHEDULED_START_TIME = I_WORK_SCHEDULE.SCHEDULED_START_TIME,
                    WS.SCHEDULED_END_TIME = I_WORK_SCHEDULE.SCHEDULED_END_TIME,
                    WS.BREAKING_MINUTES = I_WORK_SCHEDULE.BREAKING_MINUTES,
                    WS.LAST_UPDATE_DATE = SYSTIMESTAMP WHEN NOT MATCHED THEN INSERT (
                        EMPLOYEE_ID,
                        ATTENDANCE_DATE,
                        SCHEDULED_START_TIME,
                        SCHEDULED_END_TIME,
                        BREAKING_MINUTES,
                        CREATION_DATE,
                        LAST_UPDATE_DATE
                    ) VALUES (
                        I_WORK_SCHEDULE.EMPLOYEE_ID,
                        I_WORK_SCHEDULE.ATTENDANCE_DATE,
                        I_WORK_SCHEDULE.SCHEDULED_START_TIME,
                        I_WORK_SCHEDULE.SCHEDULED_END_TIME,
                        I_WORK_SCHEDULE.BREAKING_MINUTES,
                        SYSTIMESTAMP,
                        SYSTIMESTAMP
                    );
            END IF;
 

            -- TIMECARDSテーブルの更新
            IF I_TIMECARD.EMPLOYEE_ID IS NOT NULL AND I_TIMECARD.ATTENDANCE_DATE IS NOT NULL THEN
                MERGE INTO TIMECARDS TC USING (
                    SELECT
                        I_TIMECARD.EMPLOYEE_ID          AS EMPLOYEE_ID,
                        I_TIMECARD.ATTENDANCE_DATE      AS ATTENDANCE_DATE
                    FROM
                        DUAL
                ) I ON (TC.EMPLOYEE_ID = I.EMPLOYEE_ID
                AND TC.ATTENDANCE_DATE = I.ATTENDANCE_DATE) WHEN MATCHED THEN
                    UPDATE
                    SET
                        TC.START_TIME = I_TIMECARD.START_TIME,
                        TC.END_TIME = I_TIMECARD.END_TIME,
                        TC.LAST_UPDATE_DATE = SYSTIMESTAMP WHEN NOT MATCHED THEN INSERT (
                            EMPLOYEE_ID,
                            ATTENDANCE_DATE,
                            START_TIME,
                            END_TIME,
                            CREATION_DATE,
                            LAST_UPDATE_DATE
                        ) VALUES (
                            I_TIMECARD.EMPLOYEE_ID,
                            I_TIMECARD.ATTENDANCE_DATE,
                            I_TIMECARD.START_TIME,
                            I_TIMECARD.END_TIME,
                            SYSTIMESTAMP,
                            SYSTIMESTAMP
                        );
                END IF;
            END MERGE_MULTI_TABLE_DATA;
 

            -- データをエクスポートする手続き
            PROCEDURE EXPORT(
                I_DATE_FROM IN DATE,
                I_DATE_TO IN DATE,
                I_DIR_NAME IN VARCHAR2
            ) IS
                V_EMPLOYEE_ID_B VARCHAR2(8);
            BEGIN
                DEBUG_PRINT('エクスポート処理開始');
                DEBUG_PRINT('開始日: '
                            || TO_CHAR(I_DATE_FROM, 'YYYY/MM/DD'));
                DEBUG_PRINT('終了日: '
                            || TO_CHAR(I_DATE_TO, 'YYYY/MM/DD'));
                DEBUG_PRINT('出力先フォルダ: '
                            || I_DIR_NAME);
                CREATE_DIRECTORY(I_DIR_NAME);
 
                -- 社員マスタCSVファイルの出力
                DEBUG_PRINT('社員マスタCSVファイル出力開始');
                OPEN_FILE(CREATE_FILE_NAME('systemB_employees_'), 'W');
                PRINT_LINE(GC_EMPLOYEES_CSV_HEADER);
                FOR EMP IN G_EMPLOYEES_CUR LOOP
                    IF SUBSTR(EMP.EMPLOYEE_ID, 1, 1) = 'H' THEN
                        PRINT_LINE(EMP.EMPLOYEE_ID
                                   || ','
                                   || ','
                                   || REPLACE(EMP.FAMILY_NAME, ',', CHR(9))
                                      || ','
                                      || REPLACE(EMP.GIVEN_NAME, ',', CHR(9))
                                         || ','
                                         || EMP.IS_VALID);
                    ELSE
                        V_EMPLOYEE_ID_B := CONVERT_EMPLOYEE_ID(LTRIM(EMP.EMPLOYEE_ID, '0'));
                        PRINT_LINE(LTRIM(EMP.EMPLOYEE_ID, '0')
                                   || ','
                                   || V_EMPLOYEE_ID_B
                                   || ','
                                   || REPLACE(EMP.FAMILY_NAME, ',', CHR(9))
                                      || ','
                                      || REPLACE(EMP.GIVEN_NAME, ',', CHR(9))
                                         || ','
                                         || EMP.IS_VALID);
                    END IF;
                END LOOP;

                CLOSE_FILE;
                DEBUG_PRINT('社員マスタCSVファイル出力終了');
 
                -- 勤怠データCSVファイルの出力
                DEBUG_PRINT('勤怠データCSVファイル出力開始');
                OPEN_FILE(CREATE_FILE_NAME('systemB_attendances_'), 'W');
                PRINT_LINE(GC_ATTENDANCES_CSV_HEADER);
                FOR ATT IN G_ATTENDANCES_CUR(I_DATE_FROM, I_DATE_TO) LOOP
                    IF SUBSTR(ATT.EMPLOYEE_ID, 1, 1) = 'H' THEN
                        V_EMPLOYEE_ID_B := '';
                    ELSE
                        V_EMPLOYEE_ID_B := CONVERT_EMPLOYEE_ID(LTRIM(ATT.EMPLOYEE_ID, '0'));
                    END IF;

                    PRINT_LINE(LTRIM(ATT.EMPLOYEE_ID, '0')
                               || ','
                               || V_EMPLOYEE_ID_B
                               || ','
                               || TO_CHAR(ATT.ATTENDANCE_DATE, 'YYYY/MM/DD')
                                  || ','
                                  || NVL(ATT.ATTENDANCE_TYPE_CODE, '')
                                     || ','
                                     || NVL(ATT.SCHEDULED_START_TIME, '')
                                        || ','
                                        || NVL(ATT.SCHEDULED_END_TIME, '')
                                           || ','
                                           || NVL(ATT.START_TIME, '')
                                              || ','
                                              || NVL(ATT.END_TIME, '')
                                                 || ','
                                                 || ATT.WORKING_MINUTES
                                                 || ','
                                                 || NVL(ATT.BREAKING_MINUTES, 0)
                                                    || ','
                                                    || ATT.ABSENCE_MINUTES
                                                    || ','
                                                    || ATT.OVER_TIME_MINUTES
                                                    || ','
                                                    || ATT.MIDNIGHT_WORKING_MINUTES
                                                    || ','
                                                    || ATT.OVER_60_OVER_TIME_MINUTES);
                END LOOP;

                CLOSE_FILE;
                DEBUG_PRINT('勤怠データCSVファイル出力終了');
                DROP_DIRECTORY;
                DEBUG_PRINT('エクスポート処理終了');
            EXCEPTION
                WHEN OTHERS THEN
                    DEBUG_PRINT('エラーが発生しました。ロールバックします。');
                    CLOSE_FILE;
                    DROP_DIRECTORY;
                    RAISE;
            END EXPORT;
 

            -- データをインポートする手続き
            PROCEDURE IMPORT(
                I_DIR_NAME IN VARCHAR2,
                I_EMPLOYEES_FILE IN VARCHAR2,
                I_ATTENDANCES_FILE IN VARCHAR2
            ) IS
                V_LINE           VARCHAR2(4000);
                V_EMP_LINE_COUNT NUMBER := 0;
                V_ATT_LINE_COUNT NUMBER := 0;
            BEGIN
                DEBUG_PRINT('インポート処理開始');
                DEBUG_PRINT('入力元フォルダ: '
                            || I_DIR_NAME);
                DEBUG_PRINT('社員マスタファイル: '
                            || I_EMPLOYEES_FILE);
                DEBUG_PRINT('勤怠データファイル: '
                            || I_ATTENDANCES_FILE);
                CREATE_DIRECTORY(I_DIR_NAME);
 
                -- 社員データの処理
                DEBUG_PRINT('社員マスタCSVファイル読み込み開始');
                BEGIN
                    OPEN_FILE(I_EMPLOYEES_FILE, 'R');
                    IF GET_LINE = GC_EMPLOYEES_CSV_HEADER THEN
                        LOOP
                            BEGIN
                                V_LINE := GET_LINE;
                                V_EMP_LINE_COUNT := V_EMP_LINE_COUNT + 1;
                                MERGE_EMPLOYEE(TO_EMPLOYEE(V_LINE));
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    DEBUG_PRINT(V_EMP_LINE_COUNT
                                                || '行の社員データを読み込みました。');
                                    EXIT;
                            END;
                        END LOOP;
                    ELSE
                        RAISE_APPLICATION_ERROR(-20001, '"'
                                                        || I_EMPLOYEES_FILE
                                                        || '"は社員マスタのCSVファイルではありません。');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DEBUG_PRINT('社員マスタファイル読み込み中にエラーが発生しました: '
                                    || SQLERRM);
                        RAISE;
                END;

                CLOSE_FILE;
                DEBUG_PRINT('社員マスタCSVファイル読み込み終了');
 
                -- 勤怠データの処理
                DEBUG_PRINT('勤怠データCSVファイル読み込み開始');
                BEGIN
                    OPEN_FILE(I_ATTENDANCES_FILE, 'R');
                    IF GET_LINE = GC_ATTENDANCES_CSV_HEADER THEN
                        LOOP
                            BEGIN
                                V_LINE := GET_LINE;
                                V_ATT_LINE_COUNT := V_ATT_LINE_COUNT + 1;
                                DECLARE
                                    L_ATTENDANCE ATTENDANCES%ROWTYPE;
                                BEGIN
                                    L_ATTENDANCE := TO_ATTENDANCE(V_LINE);
                                    IF L_ATTENDANCE.EMPLOYEE_ID IS NOT NULL THEN
                                        MERGE_MULTI_TABLE_DATA(L_ATTENDANCE, NULL, NULL);
                                    ELSE
                                        DEBUG_PRINT('無効なレコードをスキップします。行番号: '
                                                    || V_ATT_LINE_COUNT);
                                    END IF;
                                END;
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    DEBUG_PRINT(V_ATT_LINE_COUNT
                                                || '行の勤怠データを読み込みました。');
                                    EXIT;
                            END;
                        END LOOP;
                    ELSE
                        RAISE_APPLICATION_ERROR(-20002, '"'
                                                        || I_ATTENDANCES_FILE
                                                        || '"は勤怠データのCSVファイルではありません。');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DEBUG_PRINT('勤怠データファイル読み込み中にエラーが発生しました: '
                                    || SQLERRM);
                        RAISE;
                END;

                CLOSE_FILE;
                DEBUG_PRINT('勤怠データCSVファイル読み込み終了');
                COMMIT;
                DROP_DIRECTORY;
                DEBUG_PRINT('インポート処理終了');
            EXCEPTION
                WHEN OTHERS THEN
                    DEBUG_PRINT('エラーが発生しました。ロールバックします。');
                    DEBUG_PRINT('エラーコード: '
                                || SQLCODE);
                    DEBUG_PRINT('エラーメッセージ: '
                                || SQLERRM);
                    ROLLBACK;
                    CLOSE_FILE;
                    DROP_DIRECTORY;
                    RAISE;
            END IMPORT;
        END SYSTEM_B_IMPORT_EXPORT;
/

-- パッケージのコンパイルエラーを表示
SHOW ERRORS PACKAGE BODY SYSTEM_B_IMPORT_EXPORT;

-- テスト実行用のスクリプト
SET SERVEROUTPUT ON SIZE 1000000

DECLARE
    V_DIR_NAME         VARCHAR2(100) := 'C:\work\practice2';
    V_START_DATE       DATE := TO_DATE('2024-05-01', 'YYYY-MM-DD');
    V_END_DATE         DATE := TO_DATE('2024-05-31', 'YYYY-MM-DD');
    V_EMPLOYEES_FILE   VARCHAR2(100) := 'systemA_employees_202409131606.csv';
    V_ATTENDANCES_FILE VARCHAR2(100) := 'systemA_attendances_202409131606.csv';
BEGIN
 
    -- システムBのデータをインポート
    SYSTEM_B_IMPORT_EXPORT.IMPORT(
        I_DIR_NAME => V_DIR_NAME,
        I_EMPLOYEES_FILE => V_EMPLOYEES_FILE,
        I_ATTENDANCES_FILE => V_ATTENDANCES_FILE
    );
 
    -- システムBのデータをエクスポート
    SYSTEM_B_IMPORT_EXPORT.EXPORT(
        I_DATE_FROM => V_START_DATE,
        I_DATE_TO => V_END_DATE,
        I_DIR_NAME => V_DIR_NAME
    );
 
    -- インポート後のデータ量を検証
    FOR R IN (
        SELECT
            COUNT(*) AS EMP_COUNT
        FROM
            EMPLOYEES
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('EMPLOYEESテーブルのレコード数: '
                             || R.EMP_COUNT);
    END LOOP;

    FOR R IN (
        SELECT
            COUNT(*) AS ATT_COUNT
        FROM
            ATTENDANCES
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('ATTENDANCESテーブルのレコード数: '
                             || R.ATT_COUNT);
    END LOOP;
 

    -- WORK_SCHEDULESとTIMECARDSテーブルのデータを検証
    FOR R IN (
        SELECT
            COUNT(*) AS WS_COUNT
        FROM
            WORK_SCHEDULES
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('WORK_SCHEDULESテーブルのレコード数: '
                             || R.WS_COUNT);
    END LOOP;

    FOR R IN (
        SELECT
            COUNT(*) AS TC_COUNT
        FROM
            TIMECARDS
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('TIMECARDSテーブルのレコード数: '
                             || R.TC_COUNT);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('テスト完了');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('エラーが発生しました: '
                             || SQLERRM);
        RAISE;
END;
/

show errors;

