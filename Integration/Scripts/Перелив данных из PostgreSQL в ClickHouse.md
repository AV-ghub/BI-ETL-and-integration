# Комплексное решение для перелива данных из PostgreSQL в ClickHouse:

## 1. **Скрипт для экспорта структур из PostgreSQL**

```bash
#!/bin/bash
# export_pg_schema.sh

DB_NAME="your_database"
DB_USER="your_user"
DB_HOST="your_host"
DB_PORT="5432"

# Экспорт списка таблиц
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
" > pg_tables.list

# Экспорт структур всех таблиц
while read table; do
    echo "Экспортируем структуру таблицы: $table"
    
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
    SELECT 'CREATE TABLE ' || table_name || ' (' ||
    string_agg(column_name || ' ' || 
        CASE 
            WHEN data_type = 'integer' THEN 'Int32'
            WHEN data_type = 'bigint' THEN 'Int64'
            WHEN data_type = 'smallint' THEN 'Int16'
            WHEN data_type = 'real' THEN 'Float32'
            WHEN data_type = 'double precision' THEN 'Float64'
            WHEN data_type = 'numeric' THEN 'Decimal(' || numeric_precision || ',' || numeric_scale || ')'
            WHEN data_type = 'boolean' THEN 'UInt8'
            WHEN data_type = 'date' THEN 'Date'
            WHEN data_type = 'timestamp' THEN 'DateTime'
            WHEN data_type = 'timestamp with time zone' THEN 'DateTime'
            WHEN data_type = 'text' THEN 'String'
            WHEN data_type = 'character varying' THEN 'String'
            WHEN data_type = 'bytea' THEN 'String'
            ELSE 'String'
        END ||
        CASE WHEN is_nullable = 'YES' THEN '' ELSE ' NOT NULL' END, ', ') ||
    ') ENGINE = MergeTree() ORDER BY tuple();'
    FROM information_schema.columns 
    WHERE table_name = '$table'
    GROUP BY table_name;
    " > "ch_schema_${table}.sql"
    
done < pg_tables.list

echo "Экспорт структур завершен"
```

## 2. **Скрипт для экспорта данных из PostgreSQL**

```bash
#!/bin/bash
# export_pg_data.sh

DB_NAME="your_database"
DB_USER="your_user"
DB_HOST="your_host"
DB_PORT="5432"
EXPORT_DIR="./pg_export"

mkdir -p $EXPORT_DIR

# Экспорт данных каждой таблицы
while read table; do
    echo "Экспортируем данные таблицы: $table"
    
    # Экспорт в CSV
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
    COPY (SELECT * FROM $table) 
    TO STDOUT 
    WITH (FORMAT csv, DELIMITER '^', NULL 'NULL', ENCODING 'UTF8')
    " > "${EXPORT_DIR}/${table}.csv"
    
    # Обработка файла для ClickHouse
    awk -F '^' '{
        for (i=1; i<=NF; i++) {
            gsub(/\n/, "\\\\n", $i)  # Экранируем переносы
            gsub(/\r/, "", $i)       # Удаляем carriage return
            gsub(/"/, "'\"'\"'", $i) # Экранируем кавычки
        }
        print
    }' OFS='^' "${EXPORT_DIR}/${table}.csv" > "${EXPORT_DIR}/${table}_cleaned.csv"
    
done < pg_tables.list

echo "Экспорт данных завершен"
```

## 3. **Скрипт для импорта в ClickHouse**

```bash
#!/bin/bash
# import_to_ch.sh

CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="9000"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
EXPORT_DIR="./pg_export"

# Импорт структур
for sql_file in ch_schema_*.sql; do
    echo "Создаем таблицу из: $sql_file"
    clickhouse-client -h $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT -u $CLICKHOUSE_USER \
        --password "$CLICKHOUSE_PASSWORD" -q "$(cat $sql_file)"
done

# Импорт данных
for csv_file in ${EXPORT_DIR}/*_cleaned.csv; do
    table=$(basename $csv_file "_cleaned.csv")
    echo "Импортируем данные в таблицу: $table"
    
    clickhouse-client -h $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT -u $CLICKHOUSE_USER \
        --password "$CLICKHOUSE_PASSWORD" -q "
        INSERT INTO $table 
        FORMAT CSVWithNames
        SETTINGS 
            format_csv_delimiter = '^',
            input_format_allow_errors_num = 1000,
            input_format_allow_errors_ratio = 0.1
        " < "$csv_file"
done

echo "Импорт в ClickHouse завершен"
```

## 4. **Универсальный скрипт для конвертации типов**

```python
#!/usr/bin/env python3
# pg_to_ch_converter.py

import psycopg2
from clickhouse_driver import Client

def get_pg_schema(connection_string):
    """Получить схему PostgreSQL"""
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT table_name, column_name, data_type, 
           is_nullable, numeric_precision, numeric_scale,
           character_maximum_length
    FROM information_schema.columns 
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position
    """)
    
    schema = {}
    for row in cursor.fetchall():
        table, column, data_type, nullable, num_prec, num_scale, char_max = row
        
        if table not in schema:
            schema[table] = []
        
        # Конвертация типов PostgreSQL -> ClickHouse
        ch_type = convert_type(data_type, nullable, num_prec, num_scale, char_max)
        schema[table].append(f"{column} {ch_type}")
    
    return schema

def convert_type(pg_type, nullable, num_prec, num_scale, char_max):
    """Конвертировать тип PostgreSQL в ClickHouse"""
    type_map = {
        'integer': 'Int32',
        'bigint': 'Int64', 
        'smallint': 'Int16',
        'real': 'Float32',
        'double precision': 'Float64',
        'numeric': f'Decimal({num_prec}, {num_scale})',
        'boolean': 'UInt8',
        'date': 'Date',
        'timestamp': 'DateTime',
        'timestamp with time zone': 'DateTime',
        'text': 'String',
        'character varying': 'String',
        'character': f'FixedString({char_max})',
        'bytea': 'String',
        'json': 'String',
        'jsonb': 'String',
        'uuid': 'UUID'
    }
    
    ch_type = type_map.get(pg_type, 'String')
    if nullable == 'YES':
        ch_type = f'Nullable({ch_type})'
    
    return ch_type

def generate_ch_ddl(schema):
    """Сгенерировать DDL для ClickHouse"""
    ddl_statements = []
    for table, columns in schema.items():
        columns_str = ', '.join(columns)
        ddl = f"CREATE TABLE {table} ({columns_str}) ENGINE = MergeTree() ORDER BY tuple()"
        ddl_statements.append(ddl)
    
    return ddl_statements

# Использование
if __name__ == "__main__":
    pg_conn_str = "host=localhost dbname=your_db user=your_user password=your_pass"
    
    schema = get_pg_schema(pg_conn_str)
    ddl_statements = generate_ch_ddl(schema)
    
    for ddl in ddl_statements:
        print(ddl + ';')
```

## 5. **Параллельный экспорт данных**

```bash
#!/bin/bash
# parallel_export.sh

DB_NAME="your_database"
DB_USER="your_user"
DB_HOST="your_host"
EXPORT_DIR="./pg_export"

# Функция для экспорта одной таблицы
export_table() {
    table=$1
    echo "Экспорт: $table"
    
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
    COPY (SELECT * FROM $table) 
    TO STDOUT 
    WITH (FORMAT csv, DELIMITER '^', NULL 'NULL', ENCODING 'UTF8')
    " > "${EXPORT_DIR}/${table}.csv"
}

export -f export_table

# Параллельный экспорт всех таблиц
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
" | parallel -j 4 export_table {}
```

## 6. **Автоматизация всего процесса**

```bash
#!/bin/bash
# migrate_pg_to_ch.sh

# Настройки
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="source_db"
PG_USER="user"
PG_PASS="password"

CH_HOST="localhost"
CH_PORT="9000"
CH_DB="target_db"
CH_USER="default"
CH_PASS=""

EXPORT_DIR="./migration_export"
mkdir -p $EXPORT_DIR

echo "=== Миграция из PostgreSQL в ClickHouse ==="

# 1. Экспорт структуры
echo "1. Экспорт структуры PostgreSQL..."
psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -t -c "
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
" > $EXPORT_DIR/tables.list

# 2. Генерация DDL для ClickHouse
echo "2. Генерация DDL для ClickHouse..."
while read table; do
    psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -t -c "
    SELECT 'CREATE TABLE $table (' ||
    string_agg(column_name || ' ' || 
        CASE 
            WHEN data_type = 'integer' THEN 'Int32'
            WHEN data_type = 'bigint' THEN 'Int64'
            WHEN data_type = 'smallint' THEN 'Int16'
            WHEN data_type = 'real' THEN 'Float32'
            WHEN data_type = 'double precision' THEN 'Float64'
            WHEN data_type = 'numeric' THEN 'Decimal(' || numeric_precision || ',' || numeric_scale || ')'
            WHEN data_type = 'boolean' THEN 'UInt8'
            WHEN data_type = 'date' THEN 'Date'
            WHEN data_type = 'timestamp' THEN 'DateTime'
            WHEN data_type = 'text' THEN 'String'
            WHEN data_type = 'character varying' THEN 'String'
            ELSE 'String'
        END, ', ') ||
    ') ENGINE = MergeTree() ORDER BY tuple();'
    FROM information_schema.columns 
    WHERE table_name = '$table'
    GROUP BY table_name;
    " > "$EXPORT_DIR/ch_$table.sql"
done < $EXPORT_DIR/tables.list

# 3. Экспорт данных
echo "3. Экспорт данных..."
while read table; do
    echo "  - $table"
    psql "postgresql://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB" -c "
    COPY (SELECT * FROM $table) TO STDOUT 
    WITH (FORMAT csv, DELIMITER '^', NULL 'NULL', ENCODING 'UTF8')
    " | awk '{gsub(/\n/,"\\n"); gsub(/\r/,""); print}' > "$EXPORT_DIR/$table.csv"
done < $EXPORT_DIR/tables.list

# 4. Импорт в ClickHouse
echo "4. Импорт в ClickHouse..."
while read table; do
    echo "  - $table"
    # Создание таблицы
    clickhouse-client -h $CH_HOST --port $CH_PORT -u $CH_USER --password "$CH_PASS" \
        -q "$(cat $EXPORT_DIR/ch_$table.sql)"
    
    # Импорт данных
    clickhouse-client -h $CH_HOST --port $CH_PORT -u $CH_USER --password "$CH_PASS" -q "
    INSERT INTO $table FORMAT CSV
    SETTINGS 
        format_csv_delimiter = '^',
        input_format_allow_errors_num = 1000,
        input_format_allow_errors_ratio = 0.1
    " < "$EXPORT_DIR/$table.csv"
done < $EXPORT_DIR/tables.list

echo "Миграция завершена!"
```

## 7. **Дополнительные утилиты**

Установите необходимые инструменты:
```bash
# Для Ubuntu/Debian
sudo apt-get install postgresql-client clickhouse-client parallel awk

# Для CentOS/RHEL
sudo yum install postgresql clickhouse-client parallel gawk
```
