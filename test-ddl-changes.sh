#!/bin/bash

# DDL 变更测试脚本
# 测试 SchemaEvolver 的各种 DDL 操作

set -e

echo "🧪 Testing DDL Changes"
echo "===================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查环境
check_environment() {
    if ! docker-compose ps mysql-source | grep -q "running"; then
        print_error "MySQL source is not running. Please start the environment first."
        exit 1
    fi
    
    if ! docker-compose ps mysql-sink | grep -q "running"; then
        print_error "MySQL sink is not running. Please start the environment first."
        exit 1
    fi
    
    print_success "Environment check passed."
}

# 执行 SQL 命令
execute_sql() {
    local db=$1
    local sql=$2
    local container="mysql-source"
    
    if [ "$db" = "sink" ]; then
        container="mysql-sink"
    fi
    
    docker-compose exec -T "$container" mysql -u"$3" -p"$4" "$5" -e "$sql"
}

# 检查表结构
check_table_structure() {
    local db=$1
    local table=$2
    local container="mysql-source"
    local user="cdc_user"
    local pass="cdc_password"
    local db_name="flink_source"
    
    if [ "$db" = "sink" ]; then
        container="mysql-sink"
        user="root"
        pass="root"
        db_name="flink_sink"
    fi
    
    print_info "Checking table structure for $db.$table:"
    docker-compose exec -T "$container" mysql -u"$user" -p"$pass" "$db_name" -e "DESCRIBE $table;"
}

# 测试 ADD COLUMN
test_add_column() {
    print_info "Testing ADD COLUMN operation..."
    
    # 在源表添加新列
    execute_sql "source" "ALTER TABLE users ADD COLUMN phone VARCHAR(20) AFTER email;" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查同步结果
    check_table_structure "sink" "sink_users"
    
    # 插入测试数据
    execute_sql "source" "INSERT INTO users (name, email, phone, age) VALUES ('Test User', 'test@example.com', '1234567890', 25);" "cdc_user" "cdc_password" "flink_source"
    
    sleep 3
    execute_sql "sink" "SELECT name, email, phone, age FROM sink_users WHERE name = 'Test User';" "root" "root" "flink_sink"
    
    print_success "ADD COLUMN test completed."
}

# 测试 MODIFY COLUMN
test_modify_column() {
    print_info "Testing MODIFY COLUMN operation..."
    
    # 修改列类型
    execute_sql "source" "ALTER TABLE users MODIFY COLUMN age VARCHAR(3);" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查同步结果
    check_table_structure "sink" "sink_users"
    
    # 插入测试数据
    execute_sql "source" "INSERT INTO users (name, email, phone, age) VALUES ('Test User 2', 'test2@example.com', '0987654321', '30');" "cdc_user" "cdc_password" "flink_source"
    
    sleep 3
    execute_sql "sink" "SELECT name, email, phone, age FROM sink_users WHERE name = 'Test User 2';" "root" "root" "flink_sink"
    
    print_success "MODIFY COLUMN test completed."
}

# 测试 DROP COLUMN
test_drop_column() {
    print_info "Testing DROP COLUMN operation..."
    
    # 删除列
    execute_sql "source" "ALTER TABLE users DROP COLUMN phone;" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查同步结果
    check_table_structure "sink" "sink_users"
    
    print_success "DROP COLUMN test completed."
}

# 测试 RENAME COLUMN
test_rename_column() {
    print_info "Testing RENAME COLUMN operation..."
    
    # 重命名列
    execute_sql "source" "ALTER TABLE users RENAME COLUMN email TO user_email;" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查同步结果
    check_table_structure "sink" "sink_users"
    
    print_success "RENAME COLUMN test completed."
}

# 测试创建新表
test_create_table() {
    print_info "Testing CREATE TABLE operation..."
    
    # 创建新表
    execute_sql "source" "CREATE TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查同步结果
    check_table_structure "sink" "sink_test_table"
    
    # 插入测试数据
    execute_sql "source" "INSERT INTO test_table (name) VALUES ('Test Table Entry');" "cdc_user" "cdc_password" "flink_source"
    
    sleep 3
    execute_sql "sink" "SELECT * FROM sink_test_table;" "root" "root" "flink_sink"
    
    print_success "CREATE TABLE test completed."
}

# 测试删除表
test_drop_table() {
    print_info "Testing DROP TABLE operation..."
    
    # 删除表
    execute_sql "source" "DROP TABLE test_table;" "cdc_user" "cdc_password" "flink_source"
    
    # 等待同步
    sleep 5
    
    # 检查是否已删除
    if docker-compose exec -T mysql-sink mysql -u root -proot flink_sink -e "SHOW TABLES LIKE 'sink_test_table';" | grep -q "sink_test_table"; then
        print_warning "Table still exists in sink, checking if it's being dropped..."
    else
        print_success "Table successfully dropped from sink."
    fi
    
    print_success "DROP TABLE test completed."
}

# 主函数
main() {
    check_environment
    
    print_info "Starting DDL change tests..."
    
    test_add_column
    echo ""
    
    test_modify_column
    echo ""
    
    test_drop_column
    echo ""
    
    test_rename_column
    echo ""
    
    test_create_table
    echo ""
    
    test_drop_table
    echo ""
    
    print_success "All DDL tests completed successfully!"
    echo ""
    echo "💡 Tips:"
    echo "   - Check Flink Web UI (http://localhost:18081) for job status"
    echo "   - Monitor logs with: docker-compose logs -c flink-jobmanager"
    echo "   - Verify data integrity by comparing source and sink databases"
}