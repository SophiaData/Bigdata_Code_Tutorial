#!/bin/bash

# 性能测试脚本
# 测试大批量数据的同步性能

set -e

echo "🚀 Performance Test"
echo "=================="

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
    
    if ! docker-compose ps flink-jobmanager | grep -q "running"; then
        print_error "Flink cluster is not running. Please start the environment first."
        exit 1
    fi
    
    print_success "Environment check passed."
}

# 生成测试数据
generate_test_data() {
    local record_count=${1:-10000}
    local batch_size=${2:-1000}
    
    print_info "Generating $record_count test records in batches of $batch_size..."
    
    # 创建临时脚本文件
    cat > /tmp/load_test_data.sql << EOF
-- Performance test data generation
-- Total records: $record_count
-- Batch size: $batch_size

SET @i = 1;
WHILE @i <= $record_count DO
    INSERT INTO users (name, email, age) 
    VALUES (CONCAT('PerfUser', @i), CONCAT('perf', @i, '@example.com'), FLOOR(18 + RAND() * 50));
    
    SET @order_count = FLOOR(1 + RAND() * 10);
    SET @j = 1;
    WHILE @j <= @order_count DO
        INSERT INTO orders (user_id, product_name, amount, status)
        VALUES (@i, CONCAT('Product', @j), FLOOR(10 + RAND() * 500), 
               ELT(FLOOR(1 + RAND() * 5), 'completed', 'shipped', 'pending', 'processing', 'cancelled'));
        SET @j = @j + 1;
    END WHILE;
    
    SET @i = @i + 1;
    
    -- Commit every batch
    IF @i % $batch_size = 0 THEN
        COMMIT;
        SET @batch_num = @i / $batch_size;
        SELECT CONCAT('Batch ', @batch_num, ' completed: ', @i, ' records inserted') AS progress;
    END IF;
END WHILE;

COMMIT;
SELECT CONCAT('Total: ', @i - 1, ' records inserted') AS total;
EOF

    # 执行数据加载
    print_info "Loading test data into source database..."
    docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source < /tmp/load_test_data.sql
    
    print_success "Test data generated successfully."
}

# 监控同步性能
monitor_sync() {
    local duration=${1:-300}  # 5 minutes default
    
    print_info "Monitoring sync performance for $duration seconds..."
    
    # 初始计数
    local source_users=$(docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "SELECT COUNT(*) as users FROM users;" | tail -1)
    local source_orders=$(docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "SELECT COUNT(*) as orders FROM orders;" | tail -1)
    
    local sink_users=$(docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "SELECT COUNT(*) as users FROM sink_users;" | tail -1)
    local sink_orders=$(docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "SELECT COUNT(*) as orders FROM sink_orders;" | tail -1)
    
    echo "Initial counts:"
    echo "  Source: $source_users users, $source_orders orders"
    echo "  Sink:   $sink_users users, $sink_orders orders"
    echo ""
    
    # 监控循环
    local start_time=$(date +%s)
    local last_check=0
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $duration ]; then
            break
        fi
        
        # 每10秒检查一次
        if [ $((current_time - last_check)) -ge 10 ]; then
            local current_source_users=$(docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "SELECT COUNT(*) FROM users;" | tail -1)
            local current_sink_users=$(docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "SELECT COUNT(*) FROM sink_users;" | tail -1)
            
            local sync_rate=$(( (current_sink_users - sink_users) / (elapsed / 60) ))
            
            echo "[$elapsed/$duration] Sync progress: $current_sink_users/$current_source_users users (Rate: $sync_rate/min)"
            
            last_check=$current_time
        fi
        
        sleep 1
    done
    
    # 最终计数
    local final_source_users=$(docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "SELECT COUNT(*) FROM users;" | tail -1)
    local final_sink_users=$(docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "SELECT COUNT(*) FROM sink_users;" | tail -1)
    
    echo ""
    echo "Final counts:"
    echo "  Source: $final_source_users users"
    echo "  Sink:   $final_sink_users users"
    
    if [ "$final_sink_users" = "$final_source_users" ]; then
        print_success "All data synchronized successfully!"
    else
        print_warning "Data sync incomplete. Check for errors."
    fi
}

# 测试不同批量大小
test_batch_performance() {
    print_info "Testing different batch sizes..."
    
    # 测试不同的批量配置
    local batch_sizes=("100" "500" "1000" "2000")
    
    for batch_size in "${batch_sizes[@]}"; do
        echo ""
        print_info "Testing with batch size: $batch_size"
        
        # 生成少量测试数据
        generate_test_data 1000 $batch_size
        
        # 监控同步
        monitor_sync 60
        
        # 清理测试数据
        print_info "Cleaning up test data..."
        docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "DELETE FROM orders WHERE user_id > 1000;"
        docker-compose exec -T mysql-source mysql -u cdc_user -p cdc_password flink_source -e "DELETE FROM users WHERE id > 1000;"
        docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "DELETE FROM sink_orders WHERE user_id > 1000;"
        docker-compose exec -T mysql-sink mysql -u root -p root flink_sink -e "DELETE FROM sink_users WHERE id > 1000;"
    done
}

# 内存和CPU监控
monitor_resources() {
    print_info "Monitoring system resources..."
    
    # 监控 Flink 容器资源使用
    echo "Flink JobManager resources:"
    docker stats flink-jobmanager --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}"
    
    echo "Flink TaskManager resources:"
    docker stats flink-taskmanager --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}"
    
    echo "MySQL resources:"
    docker stats mysql-source mysql-sink --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}"
}

# 主函数
main() {
    local test_type=${1:-basic}
    local record_count=${2:-10000}
    local duration=${3:-300}
    
    check_environment
    
    case $test_type in
        "basic")
            print_info "Running basic performance test with $record_count records..."
            generate_test_data $record_count 1000
            monitor_sync $duration
            ;;
        "batch")
            print_info "Running batch performance test..."
            test_batch_performance
            ;;
        "stress")
            print_info "Running stress test with $record_count records..."
            generate_test_data $record_count 500
            monitor_sync $duration
            monitor_resources
            ;;
        *)
            print_error "Unknown test type: $test_type"
            echo "Usage: $0 {basic|batch|stress} [record_count] [duration]"
            exit 1
            ;;
    esac
    
    print_success "Performance test completed!"
    echo ""
    echo "📊 Performance metrics:"
    echo "   - Check Flink Web UI for detailed metrics"
    echo "   - Monitor logs: docker-compose logs -f flink-jobmanager"
    echo "   - Check database performance: docker-compose exec mysql-source mysql -e 'SHOW PROCESSLIST;'"
}