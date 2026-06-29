#!/bin/bash

# Flink CDC 整库同步测试脚本
# 一键启动所有服务并运行测试

set -e

echo "🚀 Starting Flink CDC Whole Database Sync Test"
echo "============================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函数：打印带颜色的信息
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

# 检查 Docker 和 Docker Compose
check_dependencies() {
    print_info "Checking dependencies..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_success "Docker and Docker Compose are installed."
}

# 创建必要的目录
create_directories() {
    print_info "Creating necessary directories..."
    
    mkdir -p test-data
    mkdir -p docker-init
    
    # 创建初始化脚本
    cat > docker-init/init-source.sql << 'EOF'
-- Create test tables for source database
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product_name VARCHAR(200) NOT NULL,
    amount DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id)
);

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2),
    stock INT DEFAULT 0,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email, age) VALUES 
('Alice', 'alice@example.com', 25),
('Bob', 'bob@example.com', 30),
('Charlie', 'charlie@example.com', 35);

INSERT INTO orders (user_id, product_name, amount, status) VALUES 
(1, 'Laptop', 999.99, 'completed'),
(1, 'Mouse', 29.99, 'shipped'),
(2, 'Keyboard', 79.99, 'pending'),
(3, 'Monitor', 299.99, 'completed');

INSERT INTO products (name, price, stock, category) VALUES 
('Laptop', 999.99, 10, 'Electronics'),
('Mouse', 29.99, 50, 'Electronics'),
('Keyboard', 79.99, 30, 'Electronics'),
('Monitor', 299.99, 15, 'Electronics');
EOF

    # 创建测试数据脚本
    cat > test-data/init-data.sql << 'EOF'
-- Additional test data for performance testing
INSERT INTO users (name, email, age) VALUES 
('David', 'david@example.com', 28),
('Eve', 'eve@example.com', 32),
('Frank', 'frank@example.com', 29);

INSERT INTO orders (user_id, product_name, amount, status) VALUES 
(4, 'Headphones', 199.99, 'completed'),
(4, 'Webcam', 89.99, 'shipped'),
(5, 'Tablet', 399.99, 'pending');

-- Performance test data (1000 records)
-- This will be used for load testing
SET @i = 6;
WHILE @i <= 1000 DO
    INSERT INTO users (name, email, age) 
    VALUES (CONCAT('User', @i), CONCAT('user', @i, '@example.com'), FLOOR(18 + RAND() * 50));
    
    SET @user_id = LAST_INSERT_ID();
    SET @j = 1;
    WHILE @j <= 5 DO
        INSERT INTO orders (user_id, product_name, amount, status)
        VALUES (@user_id, CONCAT('Product', @j), FLOOR(10 + RAND() * 500), 
               ELT(@j, 'completed', 'shipped', 'pending', 'processing', 'cancelled'));
        SET @j = @j + 1;
    END WHILE;
    
    SET @i = @i + 1;
END WHILE;
EOF

    print_success "Directories and initialization scripts created."
}

# 启动服务
start_services() {
    print_info "Starting Docker services..."
    
    # 启动 MySQL 服务
    print_info "Starting MySQL databases..."
    docker-compose up -d mysql-source mysql-sink
    
    # 等待 MySQL 启动
    print_info "Waiting for MySQL databases to be ready..."
    sleep 10
    
    # 检查 MySQL 健康状态
    while ! docker-compose exec -T mysql-source mysqladmin ping -u root -proot --silent; do
        print_info "Waiting for MySQL source..."
        sleep 2
    done
    
    while ! docker-compose exec -T mysql-sink mysqladmin ping -u root -proot --silent; do
        print_info "Waiting for MySQL sink..."
        sleep 2
    done
    
    print_success "MySQL databases are ready."
    
    # 启动 Flink 服务
    print_info "Starting Flink cluster..."
    docker-compose up -d flink-jobmanager flink-taskmanager
    
    # 等待 Flink 启动
    print_info "Waiting for Flink cluster to be ready..."
    sleep 30
    
    # 检查 Flink 健康状态
    while ! curl -f http://localhost:18081/cluster > /dev/null 2>&1; do
        print_info "Waiting for Flink cluster..."
        sleep 5
    done
    
    print_success "Flink cluster is ready."
    
    # 启动测试数据加载器
    print_info "Loading test data..."
    docker-compose up test-data-loader
    print_success "Test data loaded."
}

# 构建和部署 Flink 作业
deploy_job() {
    print_info "Building and deploying Flink job..."
    
    # 构建项目
    print_info "Building project..."
    ./mvnw -DskipTests clean package -pl sync_database_mysql
    
    # 复制 JAR 文件到 Docker
    print_info "Copying JAR to Flink container..."
    docker cp sync_database_mysql/target/sync_database_mysql-1.0.0-jar-with-dependencies.jar flink-jobmanager:/app/
    
    # 提交 Flink 作业
    print_info "Submitting Flink job..."
    docker-compose exec flink-jobmanager \
        flink run -d \
        -c io.sophiadata.flink.sync.FlinkSqlWDS \
        -p 1 \
        --jobname "CDC-Whole-Database-Sync" \
        /app/sync_database_mysql-1.0.0-jar-with-dependencies.jar \
        --config /app/config.properties
    
    print_success "Flink job deployed successfully."
}

# 验证部署
verify_deployment() {
    print_info "Verifying deployment..."
    
    # 检查 Flink Web UI
    print_info "Flink Web UI: http://localhost:18081"
    
    # 检查作业状态
    print_info "Checking job status..."
    docker-compose exec flink-jobmanager flink list
    
    # 检查数据库表
    print_info "Checking source database tables..."
    docker-compose exec -T mysql-source mysql -u cdc_user -pcdc_password flink_source -e "SHOW TABLES;"
    
    print_info "Checking sink database tables..."
    docker-compose exec -T mysql-sink mysql -u root -proot flink_sink -e "SHOW TABLES;"
}

# 显示使用说明
show_instructions() {
    echo ""
    echo "🎉 Flink CDC 整库同步环境已启动成功！"
    echo "============================================="
    echo ""
    echo "📊 访问地址："
    echo "   - Flink Web UI: http://localhost:18081"
    echo "   - MySQL Source: localhost:33061"
    echo "   - MySQL Sink: localhost:33062"
    echo ""
    echo "🔧 常用命令："
    echo "   - 查看日志: docker-compose logs -f [service_name]"
    echo "   - 停止服务: docker-compose down"
    echo "   - 重启服务: docker-compose restart [service_name]"
    echo "   - 查看作业: docker-compose exec flink-jobmanager flink list"
    echo ""
    echo "📝 测试建议："
    echo "   1. 在 Flink Web UI 中查看作业状态"
    echo "   2. 在 MySQL Source 中执行 INSERT/UPDATE/DELETE 操作"
    echo "   3. 在 MySQL Sink 中验证数据同步"
    echo "   4. 执行 DDL 操作测试 schema 变更同步"
    echo ""
    echo "🧪 测试脚本："
    echo "   - ./test-ddl-changes.sh  # 测试 DDL 变更"
    echo "   - ./test-load.sh         # 测试性能"
    echo ""
}

# 清理函数
cleanup() {
    print_info "Cleaning up..."
    docker-compose down -v
    print_success "Cleanup completed."
}

# 主函数
main() {
    case "${1:-start}" in
        "start")
            check_dependencies
            create_directories
            start_services
            deploy_job
            verify_deployment
            show_instructions
            ;;
        "stop")
            cleanup
            ;;
        "restart")
            cleanup
            main "start"
            ;;
        "logs")
            docker-compose logs -f
            ;;
        "status")
            docker-compose ps
            ;;
        *)
            echo "Usage: $0 {start|stop|restart|logs|status}"
            echo ""
            echo "  start   - Start the entire environment (default)"
            echo "  stop    - Stop and remove all containers"
            echo "  restart - Restart the environment"
            echo "  logs    - Show logs"
            echo "  status  - Show container status"
            exit 1
            ;;
    esac
}

# 捕获中断信号
trap 'print_warning "Received interrupt signal, cleaning up..."; cleanup; exit 0' INT TERM

# 运行主函数
main "$@"