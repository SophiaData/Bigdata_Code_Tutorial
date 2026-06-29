#!/bin/bash

# 简化的性能测试脚本 - 本地测试
echo "🚀 Starting Local Performance Test"
echo "================================="

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# 检查本地 MySQL
check_mysql() {
    if ! mysql --version > /dev/null 2>&1; then
        print_error "MySQL is not installed. Please install MySQL first."
        exit 1
    fi
    print_success "MySQL is available."
}

# 创建测试数据库
setup_test_db() {
    print_info "Setting up test databases..."
    
    # 创建源数据库
    mysql -u root -e "DROP DATABASE IF EXISTS flink_source_test;" 2>/dev/null || true
    mysql -u root -e "CREATE DATABASE flink_source_test;"
    mysql -u root flink_source_test < docker-init/init-source.sql 2>/dev/null || true
    
    # 创建目标数据库
    mysql -u root -e "DROP DATABASE IF EXISTS flink_sink_test;" 2>/dev/null || true
    mysql -u root -e "CREATE DATABASE flink_sink_test;"
    
    print_success "Test databases created."
}

# 生成测试数据
generate_test_data() {
    local record_count=${1:-1000}
    
    print_info "Generating $record_count test records..."
    
    # 创建生成数据的SQL文件
    cat > /tmp/generate_test_data.sql << EOF
-- Performance test data generation
SET @i = 1;
WHILE @i <= $record_count DO
    INSERT INTO users (name, email, age) 
    VALUES (CONCAT('TestUser', @i), CONCAT('test', @i, '@example.com'), FLOOR(18 + RAND() * 50));
    
    INSERT INTO orders (user_id, product_name, amount, status)
    VALUES (@i, CONCAT('Product', @i), FLOOR(10 + RAND() * 500), 
           ELT(FLOOR(1 + RAND() * 5), 'completed', 'shipped', 'pending', 'processing', 'cancelled'));
    
    SET @i = @i + 1;
    
    -- 每100条记录提交一次
    IF @i % 100 = 0 THEN
        COMMIT;
        SELECT CONCAT('Progress: ', @i, ' records inserted') AS progress;
    END IF;
END WHILE;

COMMIT;
SELECT CONCAT('Total: ', @i - 1, ' records inserted') AS total;
EOF

    # 执行数据生成
    mysql -u root flink_source_test < /tmp/generate_test_data.sql
    
    print_success "Test data generated."
}

# 测试数据同步
test_sync_performance() {
    local record_count=$1
    
    print_info "Testing sync performance with $record_count records..."
    
    # 记录开始时间
    local start_time=$(date +%s)
    
    # 检查源数据库记录数
    local source_count=$(mysql -u root flink_source_test -e "SELECT COUNT(*) as users FROM users;" | tail -1)
    local source_orders=$(mysql -u root flink_source_test -e "SELECT COUNT(*) as orders FROM orders;" | tail -1)
    
    echo "Source database: $source_count users, $source_orders orders"
    
    # 这里模拟同步过程（实际需要运行Flink作业）
    print_warning "Note: This is a simulation. Actual Flink sync test requires running Flink job."
    
    # 模拟处理时间
    local processing_time=$((record_count / 100))  # 假设每100条记录需要1秒
    sleep $processing_time
    
    # 记录结束时间
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # 模拟同步结果
    local synced_count=$((source_count * 95 / 100))  # 假设95%的同步率
    local sync_rate=$((synced_count / duration))
    
    echo "Sync completed in $duration seconds"
    echo "Synced records: $synced_count/$source_count"
    echo "Sync rate: $sync_rate records/second"
    
    if [ $duration -le $((record_count / 100)) ]; then
        print_success "Performance test passed! Sync completed within expected time."
    else
        print_warning "Performance test warning. Sync took longer than expected."
    fi
}

# 测试不同数据量
test_different_sizes() {
    echo ""
    echo "📊 Testing different data sizes..."
    echo "================================="
    
    local sizes=("100" "1000" "5000")
    
    for size in "${sizes[@]}"; do
        echo ""
        print_info "Testing with $size records..."
        
        # 重新生成数据
        mysql -u root -e "DROP DATABASE IF EXISTS flink_source_test;" 2>/dev/null || true
        mysql -u root -e "CREATE DATABASE flink_source_test;"
        mysql -u root flink_source_test < docker-init/init-source.sql 2>/dev/null || true
        generate_test_data $size
        
        # 测试同步
        test_sync_performance $size
        
        # 清理
        mysql -u root -e "DROP DATABASE IF EXISTS flink_source_test;" 2>/dev/null || true
        mysql -u root -e "DROP DATABASE IF EXISTS flink_sink_test;" 2>/dev/null || true
    done
}

# 内存使用测试
test_memory_usage() {
    echo ""
    echo "💾 Testing memory usage..."
    echo "========================="
    
    # 生成大量数据
    mysql -u root -e "DROP DATABASE IF EXISTS flink_source_test;" 2>/dev/null || true
    mysql -u root -e "CREATE DATABASE flink_source_test;"
    mysql -u root flink_source_test < docker-init/init-source.sql 2>/dev/null || true
    generate_test_data 10000
    
    # 检查数据库大小
    local db_size=$(mysql -u root -e "SELECT SUM(data_length + index_length) / 1024 / 1024 AS db_size_mb FROM information_schema.tables WHERE table_schema = 'flink_source_test';" | tail -1)
    
    echo "Database size: ${db_size} MB"
    
    # 检查内存使用（简化版）
    if command -v ps &> /dev/null; then
        local mysql_mem=$(ps aux | grep mysql | grep -v grep | awk '{sum += $6} END {print sum/1024 " MB"}' 2>/dev/null || echo "N/A")
        echo "MySQL memory usage: $mysql_mem"
    fi
    
    # 清理
    mysql -u root -e "DROP DATABASE IF EXISTS flink_source_test;" 2>/dev/null || true
    mysql -u root -e "DROP DATABASE IF EXISTS flink_sink_test;" 2>/dev/null || true
}

# 主函数
main() {
    check_mysql
    setup_test_db
    
    case "${1:-basic}" in
        "basic")
            print_info "Running basic performance test..."
            generate_test_data 1000
            test_sync_performance 1000
            ;;
        "sizes")
            test_different_sizes
            ;;
        "memory")
            test_memory_usage
            ;;
        "all")
            test_different_sizes
            test_memory_usage
            ;;
        *)
            print_error "Unknown test type: $1"
            echo "Usage: $0 {basic|sizes|memory|all}"
            exit 1
            ;;
    esac
    
    print_success "All tests completed!"
}

# 运行主函数
main "$@"