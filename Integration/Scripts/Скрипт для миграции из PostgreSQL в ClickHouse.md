# Скрипт для миграции из PostgreSQL в ClickHouse:

## 1. **SQL скрипт для генерации миграции PostgreSQL → ClickHouse**

[notion](https://stackoverflow.com/questions/58023405/error-raise-when-use-within-group-with-string-agg-in-postgresql)

```sql
WITH table_columns AS (
    SELECT 
        table_schema,
        table_name,
        column_name,
        data_type,
        is_nullable,
        numeric_precision,
        numeric_scale,
        character_maximum_length,
        ordinal_position
    FROM information_schema.columns 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    -- AND table_schema = 'public'  -- раскомментируйте для конкретной схемы
),
aggregated_data AS (
    SELECT 
        table_schema,
        table_name
        ,
        -- ClickHouse DDL
        STRING_AGG(
            column_name::text || ' ' ||
            CASE data_type
                WHEN 'integer' THEN 'Int32'
                WHEN 'bigint' THEN 'Int64'
                WHEN 'smallint' THEN 'Int16'
                WHEN 'real' THEN 'Float32'
                WHEN 'double precision' THEN 'Float64'
                WHEN 'numeric' THEN 'Decimal(' || numeric_precision || ',' || numeric_scale || ')'
                WHEN 'boolean' THEN 'UInt8'
                WHEN 'date' THEN 'Date'
                WHEN 'timestamp' THEN 'DateTime'
                WHEN 'timestamp with time zone' THEN 'DateTime'
                WHEN 'time' THEN 'String'  -- ClickHouse не имеет точного аналога
                WHEN 'text' THEN 'String'
                WHEN 'character varying' THEN 'String'
                WHEN 'character' THEN 'FixedString(' || character_maximum_length || ')'
                WHEN 'bytea' THEN 'String'
                WHEN 'json' THEN 'String'
                WHEN 'jsonb' THEN 'String'
                WHEN 'uuid' THEN 'UUID'
                ELSE 'String'
            END ||
            CASE WHEN is_nullable = 'YES' THEN ' NULL' ELSE '' END
,
            ', ' ORDER BY ordinal_position
        ) /*WITHIN GROUP (ORDER BY ordinal_position)*/ AS ch_table_definition
        ,
        
        -- ClickHouse INSERT statement
        'INSERT INTO ' || table_schema || '_' || table_name || ' (' ||
        STRING_AGG(column_name::text, ', ' ORDER BY ordinal_position) /*WITHIN GROUP (ORDER BY ordinal_position)*/ || ') ' ||
        'SELECT ' || STRING_AGG(column_name, ', ' ORDER BY ordinal_position) /*WITHIN GROUP (ORDER BY ordinal_position)*/ ||
        ' FROM ' || table_schema || '_' || table_name || '_str;' AS ch_insert_statement,
        
        -- Структура для временной таблицы (все поля String)
        '(' || STRING_AGG(column_name::text || ' Nullable(String)', ', ') || ')' AS ch_string_table_definition,
        
        -- ORDER BY clause (попробуем найти первичный ключ)
        COALESCE(
            (SELECT STRING_AGG(column_name::text, ', ')
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu 
                 ON tc.constraint_name = kcu.constraint_name
             WHERE tc.table_schema = table_columns.table_schema
                 AND tc.table_name = table_columns.table_name
                 AND tc.constraint_type = 'PRIMARY KEY'
             GROUP BY tc.table_schema, tc.table_name),
            'tuple()'
        ) AS order_by_clause
        
    FROM table_columns
    GROUP BY table_schema, table_name
)
SELECT 
    -- DDL для временной таблицы (все поля String)
    'CREATE TABLE ' || table_schema || '_' || table_name || '_str ' || 
    ch_string_table_definition || 
    ' ENGINE = MergeTree() ORDER BY tuple();' AS create_temp_table_ddl,
    
    -- DDL для финальной таблицы
    'CREATE TABLE ' || table_schema || '_' || table_name || ' ' || 
    ch_table_definition || 
    ' ENGINE = MergeTree() ORDER BY ' || order_by_clause || ';' AS create_final_table_ddl,
    
    -- INSERT statement
    ch_insert_statement,
    
    -- Экспорт данных из PostgreSQL
    'COPY (SELECT * FROM ' || table_schema || '.' || table_name || ') TO STDOUT ' ||
    'WITH (FORMAT csv, DELIMITER ''^'', NULL ''NULL'', ENCODING ''UTF8'');' AS pg_export_command,
    
    -- Импорт в ClickHouse временной таблицы
    'clickhouse-client --query "INSERT INTO ' || table_schema || '_' || table_name || '_str FORMAT CSV ' ||
    'SETTINGS format_csv_delimiter=''^'', input_format_allow_errors_num=1000";' AS ch_import_temp_command,
    
    -- Импорт в финальную таблицу
    'clickhouse-client --query "' || ch_insert_statement || '"' AS ch_import_final_command
    
FROM aggregated_data
ORDER BY table_schema, table_name;
```

## 2. **Упрощенная версия для быстрой генерации**

```sql
SELECT 
    'CREATE TABLE ' || table_schema || '_' || table_name || ' (' ||
    STRING_AGG(
        column_name || ' ' ||
        CASE data_type
            WHEN 'integer' THEN 'Int32'
            WHEN 'bigint' THEN 'Int64' 
            WHEN 'smallint' THEN 'Int16'
            WHEN 'real' THEN 'Float32'
            WHEN 'double precision' THEN 'Float64'
            WHEN 'numeric' THEN 'Decimal(' || numeric_precision || ',' || numeric_scale || ')'
            WHEN 'boolean' THEN 'UInt8'
            WHEN 'date' THEN 'Date'
            WHEN 'timestamp' THEN 'DateTime'
            WHEN 'text' THEN 'String'
            WHEN 'character varying' THEN 'String'
            WHEN 'bytea' THEN 'String'
            WHEN 'uuid' THEN 'UUID'
            ELSE 'String'
        END,
        ', '
    ) WITHIN GROUP (ORDER BY ordinal_position) ||
    ') ENGINE = MergeTree() ORDER BY tuple();' AS clickhouse_ddl,
    
    'COPY (SELECT * FROM ' || table_schema || '.' || table_name || ') TO STDOUT ' ||
    'WITH (FORMAT csv, DELIMITER ''^'', NULL ''NULL'', ENCODING ''UTF8'');' AS export_command
    
FROM information_schema.columns 
WHERE table_schema = 'public'  -- укажите вашу схему
GROUP BY table_schema, table_name
ORDER BY table_schema, table_name;
```

## 3. **Bash скрипт для автоматизации миграции**

```bash
#!/bin/bash
# migrate_pg_to_ch.sh

PG_HOST="localhost"
PG_PORT="5432"
PG_DB="your_database"
PG_USER="your_user"
PG_PASS="your_password"

CH_HOST="localhost"
CH_PORT="9000"
CH_USER="default"
CH_PASS=""

# Экспорт списка таблиц
TABLES=$(psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -t -c "
SELECT table_schema || '.' || table_name 
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
AND table_type = 'BASE TABLE'
")

# Миграция каждой таблицы
for TABLE in $TABLES; do
    SCHEMA=$(echo $TABLE | cut -d'.' -f1)
    NAME=$(echo $TABLE | cut -d'.' -f2)
    
    echo "Мигрируем таблицу: $SCHEMA.$NAME"
    
    # Генерация DDL для ClickHouse
    DDL=$(psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -t -c "
    SELECT 'CREATE TABLE ' || '$SCHEMA' || '_' || '$NAME' || ' (' ||
    STRING_AGG(
        column_name || ' ' ||
        CASE data_type
            WHEN 'integer' THEN 'Int32'
            WHEN 'bigint' THEN 'Int64'
            WHEN 'smallint' THEN 'Int16'
            WHEN 'real' THEN 'Float32'
            WHEN 'double precision' THEN 'Float64'
            WHEN 'numeric' THEN 'Decimal(' || numeric_precision || ',' || numeric_scale || ')'
            WHEN 'boolean' THEN 'UInt8'
            WHEN 'date' THEN 'Date'
            WHEN 'timestamp' THEN 'DateTime'
            WHEN 'text' THEN 'String'
            WHEN 'character varying' THEN 'String'
            WHEN 'bytea' THEN 'String'
            WHEN 'uuid' THEN 'UUID'
            ELSE 'String'
        END,
        ', '
    ) WITHIN GROUP (ORDER BY ordinal_position) ||
    ') ENGINE = MergeTree() ORDER BY tuple();'
    FROM information_schema.columns 
    WHERE table_schema = '$SCHEMA' AND table_name = '$NAME'
    GROUP BY table_schema, table_name;
    ")
    
    # Создание таблицы в ClickHouse
    clickhouse-client -h $CH_HOST --port $CH_PORT -u $CH_USER --password "$CH_PASS" -q "$DDL"
    
    # Экспорт данных из PostgreSQL
    psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -c "
    COPY (SELECT * FROM $SCHEMA.$NAME) TO STDOUT 
    WITH (FORMAT csv, DELIMITER '^', NULL 'NULL', ENCODING 'UTF8')
    " | awk '{gsub(/\n/, "\\n"); gsub(/\r/, ""); print}' > "${SCHEMA}_${NAME}.csv"
    
    # Импорт в ClickHouse
    clickhouse-client -h $CH_HOST --port $CH_PORT -u $CH_USER --password "$CH_PASS" -q "
    INSERT INTO ${SCHEMA}_${NAME} FORMAT CSV
    SETTINGS format_csv_delimiter='^', input_format_allow_errors_num=1000
    " < "${SCHEMA}_${NAME}.csv"
    
    echo "Завершено: $SCHEMA.$NAME"
done
```

## 4. **Дополнительные функции для обработки**

```sql
-- Функция для получения порядка ORDER BY (первичный ключ)
CREATE OR REPLACE FUNCTION get_primary_key_columns(p_schema text, p_table text)
RETURNS text AS $$
DECLARE
    result text;
BEGIN
    SELECT STRING_AGG(column_name, ', ')
    INTO result
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema = p_schema
        AND tc.table_name = p_table
        AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY tc.table_schema, tc.table_name;
    
    RETURN COALESCE(result, 'tuple()');
END;
$$ LANGUAGE plpgsql;

-- Использование функции
SELECT get_primary_key_columns('public', 'your_table');
```

## 5. **Генерация полного скрипта миграции**

```sql
SELECT 
    '-- Миграция таблицы: ' || table_schema || '.' || table_name || '
    
-- 1. Создание таблицы в ClickHouse
CREATE TABLE ' || table_schema || '_' || table_name || ' (
    ' || STRING_AGG(
        column_name || ' ' ||
        CASE data_type
            WHEN 'integer' THEN 'Int32'
            WHEN 'bigint' THEN 'Int64'
            -- ... остальные типы
            ELSE 'String'
        END,
        ',
    '
    ) || '
) ENGINE = MergeTree() ORDER BY ' || get_primary_key_columns(table_schema, table_name) || ';

-- 2. Экспорт из PostgreSQL
COPY (SELECT * FROM ' || table_schema || '.' || table_name || ') 
TO STDOUT WITH (FORMAT csv, DELIMITER ''^'', NULL ''NULL'', ENCODING ''UTF8'');

-- 3. Импорт в ClickHouse
clickhouse-client --query "
INSERT INTO ' || table_schema || '_' || table_name || ' 
FORMAT CSV
SETTINGS format_csv_delimiter=''^'', input_format_allow_errors_num=1000
"
' AS migration_script
FROM information_schema.columns 
WHERE table_schema = 'public'
GROUP BY table_schema, table_name;
```
