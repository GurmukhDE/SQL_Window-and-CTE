# SQL Interview Questions for Data Engineers
**Complete Guide with Data Engineering Perspective**

[![SQL](https://img.shields.io/badge/SQL-Expert-blue)](https://github.com)
[![Data Engineering](https://img.shields.io/badge/Data%20Engineering-Interview%20Prep-green)](https://github.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Author**: Gurmukh Singh  
> **Purpose**: Crack NAB Data Engineer Interview  
> **Last Updated**: February 2026

---

## 📋 Table of Contents
1. [Easy Level Questions](#easy-level-questions)
   - [Find Duplicate Records](#1-find-duplicate-records)
   - [Second Highest Salary](#2-second-highest-salary)
   - [Delete Duplicates](#3-delete-duplicates-keep-one-record)
   - [Employees Earning More Than Managers](#4-employees-earning-more-than-their-managers)
2. [Medium Level Questions](#medium-level-questions)
   - [Nth Highest Salary](#5-nth-highest-salary-generic-solution)
   - [Consecutive Numbers](#6-consecutive-numbers-problem)
   - [Department Top 3 Salaries](#7-department-top-3-salaries)
   - [Running Total](#8-running-total--cumulative-sum)
   - [Month-over-Month Growth](#9-month-over-month-growth-rate)
   - [Find Customers Who Never Ordered](#10-find-customers-who-never-ordered)
3. [Hard Level Questions](#hard-level-questions)
   - [Calculate Median](#11-calculate-median-salary)
   - [User Retention Rate](#12-user-retention-rate)
   - [Trip Cancellation Rate](#13-trip-cancellation-rate)
   - [Friends Recommendations](#14-friends-recommendations-graph-problem)
   - [Pivot Table](#15-pivot-table---sales-by-product-and-month)
   - [Gap and Islands](#16-gap-and-islands-problem)
   - [Self Join Hierarchy](#17-self-join---manager-hierarchy)
   - [Top Spending Customers](#18-find-top-spending-customers-by-category)
   - [Active Subscriptions](#19-complex-date-logic---active-subscriptions)
   - [Moving Average](#20-moving-average-last-7-days)
4. [Data Engineering Patterns](#data-engineering-patterns)
5. [Performance Optimization](#performance-optimization-tips)
6. [Interview Strategy](#interview-strategy)

---

## Easy Level Questions

### 1. Find Duplicate Records

**🎯 Scenario:** You're building a user registration ETL pipeline and need to identify duplicate emails before loading into the warehouse.

**💼 Business Context:**
- Data quality check in ETL pipeline
- Prevents duplicate user accounts
- Common in data validation layers

**💡 Solution:**
```sql
-- Find users who have duplicate email addresses
SELECT 
    email, 
    COUNT(*) as duplicate_count
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

-- More detailed version for reporting
SELECT 
    email,
    COUNT(*) as duplicate_count,
    MIN(created_date) as first_occurrence,
    MAX(created_date) as last_occurrence,
    STRING_AGG(user_id::TEXT, ', ') as user_ids
FROM users
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

**🔧 Data Engineering Perspective:**

✅ **Use this in data quality checks** before loading to production tables

✅ **PySpark Implementation:**
```python
# Find duplicates in PySpark
duplicates = df.groupBy("email") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc())

duplicates.show()
```

✅ **Integrate in Airflow/Databricks pipelines** as validation step

✅ **Performance**: Index on email column for faster execution

**📊 When to Use:**
- Pre-load data validation
- Data profiling and quality reports
- Deduplication logic in staging layers

---

### 2. Second Highest Salary

**🎯 Scenario:** HR analytics dashboard needs to show salary benchmarks and outliers.

**💼 Business Context:**
- Salary analytics and compensation studies
- Identifying compensation gaps
- Department budget analysis

**💡 Solution:**
```sql
-- Method 1: Using subquery (works on all SQL variants)
SELECT MAX(salary) as second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Method 2: Using LIMIT/OFFSET (PostgreSQL, MySQL)
SELECT DISTINCT salary as second_highest_salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 1;

-- Method 3: Using DENSE_RANK (most robust for production)
WITH RankedSalaries AS (
    SELECT 
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) as salary_rank
    FROM employees
)
SELECT salary as second_highest_salary
FROM RankedSalaries
WHERE salary_rank = 2;
```

**🔧 Data Engineering Perspective:**

**⭐ Method 3 (DENSE_RANK) is preferred in production** because:
- Handles ties correctly
- More maintainable and readable
- Works across different SQL engines

**PySpark Implementation:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

window = Window.orderBy(col("salary").desc())

second_highest = df.withColumn("rank", dense_rank().over(window)) \
    .filter(col("rank") == 2) \
    .select("salary") \
    .distinct()
```

**⚡ Performance Tip:**
- Index on `salary` column
- In data warehouses, use **distribution keys** on department_id

---

### 3. Delete Duplicates (Keep One Record)

**🎯 Scenario:** Cleaning staging table after ingesting data from external API with duplicate records.

**💼 Business Context:**
- Data cleansing in staging layer
- Deduplication before loading to fact/dimension tables
- Handling upstream data quality issues

**💡 Solution:**
```sql
-- PostgreSQL method using CTID
DELETE FROM employees a
USING employees b
WHERE a.ctid < b.ctid
  AND a.email = b.email;

-- Standard SQL method (works on most databases)
DELETE FROM employees
WHERE id NOT IN (
    SELECT MIN(id)
    FROM employees
    GROUP BY email
);

-- Using CTE (more readable and safer)
WITH EmployeesToKeep AS (
    SELECT MIN(id) as id_to_keep
    FROM employees
    GROUP BY email
)
DELETE FROM employees
WHERE id NOT IN (SELECT id_to_keep FROM EmployeesToKeep);

-- For large tables: Create new table without duplicates
CREATE TABLE employees_clean AS
SELECT DISTINCT ON (email) *
FROM employees
ORDER BY email, created_date DESC;
```

**🔧 Data Engineering Best Practices:**

❌ **DON'T do this in production:**
```sql
-- Risky: No transaction, no backup
DELETE FROM employees WHERE ...;
```

✅ **DO this instead (Safe approach):**
```sql
BEGIN;

-- Step 1: Create deduplicated table
CREATE TABLE employees_deduped AS
SELECT DISTINCT ON (email) *
FROM employees
ORDER BY email, created_date DESC;

-- Step 2: Atomic table swap
ALTER TABLE employees RENAME TO employees_archived_2024_02_11;
ALTER TABLE employees_deduped RENAME TO employees;

-- Step 3: Verify, then commit
COMMIT;
```

**PySpark Production Approach:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Deduplicate keeping most recent record
window = Window.partitionBy("email").orderBy(col("created_date").desc())

df_deduped = df.withColumn("rn", row_number().over(window)) \
               .filter(col("rn") == 1) \
               .drop("rn")

# Write to new table
df_deduped.write.mode("overwrite").saveAsTable("employees_clean")
```

**📊 When to Use:**
- Staging layer cleanup after data ingestion
- Daily batch jobs to remove duplicates
- Data quality remediation in ETL pipelines

---

### 4. Employees Earning More Than Their Managers

**🎯 Scenario:** Organizational analytics - identify compensation anomalies in reporting hierarchy.

**💼 Business Context:**
- HR compliance checks
- Compensation structure validation
- Anomaly detection in payroll data

**💡 Solution:**
```sql
-- Self-join approach
SELECT 
    e.id as employee_id,
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary,
    e.salary - m.salary as salary_difference
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary
ORDER BY salary_difference DESC;

-- With additional context
SELECT 
    e.name as employee_name,
    e.salary as employee_salary,
    m.name as manager_name,
    m.salary as manager_salary,
    d.department_name,
    DATEDIFF(CURRENT_DATE, e.hire_date) as employee_tenure_days,
    ROUND((e.salary - m.salary) * 100.0 / m.salary, 2) as salary_diff_percentage
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
INNER JOIN departments d ON e.department_id = d.id
WHERE e.salary > m.salary
ORDER BY salary_diff_percentage DESC;
```

**🔧 Data Engineering Perspective:**

**Self-Join Performance:**
- **Index required on**: `manager_id`
- **Distribution strategy** (Redshift/Snowflake): Distribute on `manager_id`
- For **very large tables**: Create separate manager lookup table

**PySpark Implementation:**
```python
# Self-join in Spark
employees_as_emp = df.alias("emp")
employees_as_mgr = df.alias("mgr")

result = employees_as_emp.join(
    employees_as_mgr,
    col("emp.manager_id") == col("mgr.id"),
    "inner"
).filter(
    col("emp.salary") > col("mgr.salary")
).select(
    col("emp.name").alias("employee"),
    col("emp.salary").alias("emp_salary"),
    col("mgr.name").alias("manager"),
    col("mgr.salary").alias("mgr_salary")
)
```

**📊 Use Cases:**
- Scheduled report in Airflow for HR
- Data quality check before payroll processing
- Alert system when anomalies exceed threshold

---

## Medium Level Questions

### 5. Nth Highest Salary (Generic Solution)

**🎯 Scenario:** Building a parameterized salary reporting API that allows filtering by rank (top 5, top 10, etc.).

**💼 Business Context:**
- Compensation benchmarking across departments
- Executive dashboards showing top performers
- Salary band analysis for budget planning

**💡 Solution:**
```sql
-- Generic solution for Nth highest salary (N=3 example)
WITH RankedSalaries AS (
    SELECT 
        employee_id,
        name,
        department_id,
        salary,
        DENSE_RANK() OVER (ORDER BY salary DESC) as salary_rank
    FROM employees
)
SELECT 
    employee_id,
    name,
    salary,
    salary_rank
FROM RankedSalaries
WHERE salary_rank = 3;  -- Change to any N

-- By Department (more common in real scenarios)
WITH DepartmentSalaryRanks AS (
    SELECT 
        e.employee_id,
        e.name,
        d.department_name,
        e.salary,
        DENSE_RANK() OVER (
            PARTITION BY e.department_id 
            ORDER BY e.salary DESC
        ) as dept_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
)
SELECT 
    department_name,
    name as employee_name,
    salary,
    dept_rank
FROM DepartmentSalaryRanks
WHERE dept_rank <= 3  -- Top 3 in each department
ORDER BY department_name, dept_rank;
```

**🔧 Why DENSE_RANK vs RANK vs ROW_NUMBER?**

| Function | Use Case | Ties Handling | Example Output |
|----------|----------|---------------|----------------|
| `ROW_NUMBER()` | Unique ranking needed | Arbitrary | 1,2,3,4,5 |
| `RANK()` | Skip ranks after ties | Gaps | 1,2,2,4,5 |
| `DENSE_RANK()` | No gaps in ranking | Consecutive | 1,2,2,3,4 |

✅ **For salary analysis, use DENSE_RANK** because same salary should have same rank without gaps.

**PySpark Implementation:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

def get_nth_highest_salary(df, n, partition_col=None):
    if partition_col:
        window = Window.partitionBy(partition_col).orderBy(col("salary").desc())
    else:
        window = Window.orderBy(col("salary").desc())
    
    return df.withColumn("rank", dense_rank().over(window)) \
             .filter(col("rank") == n) \
             .drop("rank")

# Usage
top_3_per_dept = get_nth_highest_salary(employees_df, 3, "department_id")
```

---

### 6. Consecutive Numbers Problem

**🎯 Scenario:** Detecting anomalies in sensor data where the same reading appears 3+ times consecutively (sensor malfunction).

**💼 Business Context:**
- IoT sensor monitoring
- Fraud detection (repeated transaction amounts)
- System health monitoring (repeated error codes)
- Quality control (manufacturing defects)

**💡 Solution:**
```sql
-- Method 1: Self-join
SELECT DISTINCT l1.num as ConsecutiveNum
FROM Logs l1
JOIN Logs l2 ON l1.id = l2.id - 1
JOIN Logs l3 ON l1.id = l3.id - 2
WHERE l1.num = l2.num AND l2.num = l3.num;

-- Method 2: Using LAG/LEAD
WITH ConsecutiveCheck AS (
    SELECT 
        id,
        num,
        LAG(num, 1) OVER (ORDER BY id) as prev_num,
        LAG(num, 2) OVER (ORDER BY id) as prev_prev_num
    FROM Logs
)
SELECT DISTINCT num as ConsecutiveNum
FROM ConsecutiveCheck
WHERE num = prev_num AND num = prev_prev_num;

-- Method 3: Gap and Islands (most efficient)
WITH Grouped AS (
    SELECT 
        num,
        id,
        id - ROW_NUMBER() OVER (PARTITION BY num ORDER BY id) as grp
    FROM Logs
)
SELECT 
    num as ConsecutiveNum,
    MIN(id) as start_id,
    MAX(id) as end_id,
    COUNT(*) as consecutive_count
FROM Grouped
GROUP BY num, grp
HAVING COUNT(*) >= 3;
```

**🔧 Gap and Islands Pattern Explained:**

The key: `id - ROW_NUMBER()` creates same value for consecutive identical numbers.

```
id  | num | ROW_NUMBER | grp (id - ROW_NUMBER)
----|-----|------------|---------------------
1   | 5   | 1          | 0   ← Group 1
2   | 5   | 2          | 0   ← Group 1
3   | 5   | 3          | 0   ← Group 1
4   | 7   | 1          | 3   ← Group 2
5   | 5   | 4          | 1   ← Group 3
```

**PySpark Implementation:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("num").orderBy("id")

consecutive = df.withColumn("rn", row_number().over(window)) \
    .withColumn("group_id", col("id") - col("rn")) \
    .groupBy("num", "group_id") \
    .agg(
        count("*").alias("consecutive_count"),
        min("id").alias("start_id"),
        max("id").alias("end_id")
    ) \
    .filter(col("consecutive_count") >= 3)
```

**📊 Real-World Use Cases:**

1. **Fraud Detection:**
```sql
-- Alert on consecutive identical transactions
WITH TxnStreaks AS (
    SELECT 
        user_id,
        amount,
        transaction_time - ROW_NUMBER() OVER (
            PARTITION BY user_id, amount 
            ORDER BY transaction_time
        ) * INTERVAL '1 minute' as streak_group
    FROM transactions
)
SELECT user_id, amount, COUNT(*) as repeat_count
FROM TxnStreaks
GROUP BY user_id, amount, streak_group
HAVING COUNT(*) >= 3;
```

---

### 7. Department Top 3 Salaries

**🎯 Scenario:** Executive dashboard showing highest earners per department for compensation planning.

**💼 Business Context:**
- Compensation analysis across departments
- Budget allocation for salary increases
- Retention risk analysis

**💡 Solution:**
```sql
-- Top 3 earners per department
WITH DepartmentRankedSalaries AS (
    SELECT 
        d.name as department_name,
        e.name as employee_name,
        e.salary,
        DENSE_RANK() OVER (
            PARTITION BY d.id 
            ORDER BY e.salary DESC
        ) as salary_rank
    FROM employees e
    JOIN departments d ON e.department_id = d.id
)
SELECT 
    department_name,
    employee_name,
    salary,
    salary_rank
FROM DepartmentRankedSalaries
WHERE salary_rank <= 3
ORDER BY department_name, salary_rank;

-- Enhanced with metrics
WITH DepartmentStats AS (
    SELECT 
        d.name as department_name,
        e.name as employee_name,
        e.salary,
        DENSE_RANK() OVER (PARTITION BY d.id ORDER BY e.salary DESC) as salary_rank,
        AVG(e.salary) OVER (PARTITION BY d.id) as dept_avg_salary
    FROM employees e
    JOIN departments d ON e.department_id = d.id
)
SELECT 
    department_name,
    employee_name,
    salary,
    ROUND((salary - dept_avg_salary) / dept_avg_salary * 100, 2) as pct_above_avg,
    salary_rank
FROM DepartmentStats
WHERE salary_rank <= 3;
```

**🔧 Performance:**

❌ **Anti-pattern (Slow):**
```sql
-- Creates N subqueries - very inefficient
SELECT *
FROM employees e1
WHERE (
    SELECT COUNT(DISTINCT salary)
    FROM employees e2
    WHERE e1.department_id = e2.department_id AND e2.salary >= e1.salary
) <= 3;
```

✅ **Best Practice (Fast):**
```sql
-- Single pass with window function
WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as rnk
    FROM employees
)
SELECT * FROM Ranked WHERE rnk <= 3;
```

**PySpark:**
```python
from pyspark.sql.window import Window

window = Window.partitionBy("department_id").orderBy(col("salary").desc())

top_3 = df.withColumn("rank", dense_rank().over(window)) \
    .filter(col("rank") <= 3)
```

---

### 8. Running Total / Cumulative Sum

**🎯 Scenario:** Financial dashboard showing cumulative revenue, inventory tracking over time.

**💼 Business Context:**
- Revenue tracking (YTD, MTD, QTD)
- Inventory levels over time
- Campaign performance (cumulative sign-ups)

**💡 Solution:**
```sql
-- Basic running total
SELECT 
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (ORDER BY sale_date) as cumulative_sales
FROM daily_sales;

-- By category/product
SELECT 
    sale_date,
    product_id,
    daily_sales,
    SUM(daily_sales) OVER (
        PARTITION BY product_id 
        ORDER BY sale_date
    ) as product_cumulative_sales
FROM daily_sales;

-- Year-to-Date (YTD) calculations
SELECT 
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (
        PARTITION BY EXTRACT(YEAR FROM sale_date)
        ORDER BY sale_date
    ) as ytd_sales,
    SUM(daily_sales) OVER (
        PARTITION BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date)
        ORDER BY sale_date
    ) as mtd_sales
FROM daily_sales;
```

**🔧 Window Frame Specifications:**

```sql
SELECT 
    date,
    revenue,
    
    -- All rows from start to current (default)
    SUM(revenue) OVER (ORDER BY date) as cumulative,
    
    -- Last 7 days including current
    SUM(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as last_7_days,
    
    -- All rows (ignores ORDER BY)
    SUM(revenue) OVER () as grand_total
FROM daily_revenue;
```

**PySpark:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

window_spec = Window.orderBy("sale_date")

df.withColumn("running_total", spark_sum("daily_sales").over(window_spec))
```

---

### 9. Month-over-Month Growth Rate

**🎯 Scenario:** Executive dashboard tracking business growth - revenue, users, churn rate changes.

**💼 Business Context:**
- Track revenue growth trends
- Monitor KPI changes
- Identify seasonal patterns
- Alert on negative growth

**💡 Solution:**
```sql
-- Basic MoM growth
WITH MonthlyRevenue AS (
    SELECT 
        DATE_TRUNC('month', sale_date) as month,
        SUM(revenue) as monthly_revenue
    FROM sales
    GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT 
    month,
    monthly_revenue,
    LAG(monthly_revenue, 1) OVER (ORDER BY month) as prev_month_revenue,
    monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY month) as absolute_growth,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue, 1) OVER (ORDER BY month), 0),
        2
    ) as growth_percentage
FROM MonthlyRevenue;

-- MoM and YoY comparison
SELECT 
    month,
    monthly_revenue,
    LAG(monthly_revenue, 1) OVER (ORDER BY month) as mom_prev,
    LAG(monthly_revenue, 12) OVER (ORDER BY month) as yoy_prev,
    
    -- MoM Growth
    ROUND(
        (monthly_revenue - LAG(monthly_revenue, 1) OVER (ORDER BY month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue, 1) OVER (ORDER BY month), 0), 2
    ) as mom_growth_pct,
    
    -- YoY Growth
    ROUND(
        (monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue, 12) OVER (ORDER BY month), 0), 2
    ) as yoy_growth_pct
FROM MonthlyRevenue;
```

**⚠️ Common Mistakes:**

❌ **Division by zero:**
```sql
SELECT revenue / LAG(revenue) OVER (ORDER BY month) as growth  -- Will fail!
```

✅ **Correct:**
```sql
SELECT 
    revenue * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) - 100 as growth_pct
FROM monthly_revenue;
```

**PySpark:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

window = Window.orderBy("month")

df.withColumn("prev_month_revenue", lag("monthly_revenue", 1).over(window)) \
  .withColumn(
      "mom_growth",
      when(col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") != 0),
           ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100)
      )
  )
```

---

### 10. Find Customers Who Never Ordered

**🎯 Scenario:** Marketing campaign to re-engage inactive customers.

**💼 Business Context:**
- Customer retention analysis
- Marketing campaign targeting
- User onboarding funnel analysis

**💡 Solution:**
```sql
-- Method 1: LEFT JOIN (most readable)
SELECT c.customer_id, c.customer_name, c.email
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- Method 2: NOT IN (simple but slow)
SELECT customer_id, customer_name, email
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
);

-- Method 3: NOT EXISTS (most performant)
SELECT c.customer_id, c.customer_name, c.email
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- Enhanced with segmentation
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    DATEDIFF(day, c.registration_date, CURRENT_DATE) as days_since_registration,
    CASE 
        WHEN DATEDIFF(day, c.registration_date, CURRENT_DATE) < 7 THEN 'New'
        WHEN DATEDIFF(day, c.registration_date, CURRENT_DATE) < 30 THEN 'Recent'
        WHEN DATEDIFF(day, c.registration_date, CURRENT_DATE) < 90 THEN 'Moderate'
        ELSE 'Dormant'
    END as inactivity_category
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

**🔧 Performance Comparison:**

| Method | Performance | Handles NULLs | Production Use |
|--------|-------------|---------------|----------------|
| LEFT JOIN + WHERE NULL | Good | ✅ Yes | ✅ Recommended |
| NOT IN | ❌ Slow | ❌ No | ❌ Avoid |
| NOT EXISTS | ⭐ Best | ✅ Yes | ⭐ Best Choice |

**⚠️ Why NOT IN is dangerous:**
```sql
-- If orders.customer_id contains NULL, entire query returns empty!
SELECT * FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders);

-- Safe version
SELECT * FROM customers 
WHERE customer_id NOT IN (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL);
```

**PySpark (Best Method):**
```python
# Left anti join - most efficient in Spark
inactive = customers_df.join(
    orders_df.select("customer_id").distinct(),
    "customer_id",
    "left_anti"
)
```

---

## Hard Level Questions

### 11. Calculate Median Salary

**🎯 Scenario:** HR compensation analysis needs median (less affected by outliers than average).

**💼 Business Context:**
- Compensation benchmarking
- Salary band definitions
- Budget planning

**💡 Solution:**
```sql
-- Method 1: PERCENTILE_CONT (PostgreSQL, SQL Server)
SELECT 
    department_id,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary
FROM employees
GROUP BY department_id;

-- Method 2: Manual calculation (works everywhere)
WITH OrderedSalaries AS (
    SELECT 
        salary,
        ROW_NUMBER() OVER (ORDER BY salary) as row_num,
        COUNT(*) OVER () as total_count
    FROM employees
),
MedianRows AS (
    SELECT salary
    FROM OrderedSalaries
    WHERE row_num IN (
        FLOOR((total_count + 1) / 2.0),
        CEIL((total_count + 1) / 2.0)
    )
)
SELECT AVG(salary) as median_salary FROM MedianRows;

-- Percentiles (quartile analysis)
SELECT 
    department_id,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) as p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) as p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salary) as p90
FROM employees
GROUP BY department_id;
```

**💡 Why Median vs Mean?**

```
Salary Distribution: [40K, 42K, 45K, 47K, 50K, 52K, 55K, 57K, 1.5M (CEO)]

Mean: $216K ← Misleading (skewed by CEO)
Median: $50K ← True middle value
```

**Median is better for:**
- ✅ Salary distributions (executives skew average)
- ✅ House prices (luxury properties skew)
- ✅ Response times (outliers from errors)

**PySpark:**
```python
from pyspark.sql.functions import expr

# Approximate median (fast)
df.groupBy("department_id") \
  .agg(expr("percentile_approx(salary, 0.5)").alias("median_salary"))

# Exact median (slower but precise)
from pyspark.sql.window import Window

window = Window.partitionBy("department_id").orderBy("salary")
count_window = Window.partitionBy("department_id")

df.withColumn("row_num", row_number().over(window)) \
  .withColumn("total_count", count("*").over(count_window)) \
  .filter(
      (col("row_num") == floor((col("total_count") + 1) / 2)) |
      (col("row_num") == ceil((col("total_count") + 1) / 2))
  ) \
  .groupBy("department_id").agg(avg("salary").alias("median"))
```

---

### 12. User Retention Rate

**🎯 Scenario:** Calculate day-over-day user retention for product analytics.

**💼 Business Context:**
- Product engagement tracking
- Cohort analysis
- Churn prediction

**💡 Solution:**
```sql
-- Day-over-day retention
WITH UserActivity AS (
    SELECT 
        user_id,
        DATE(activity_date) as activity_date
    FROM user_logs
    GROUP BY user_id, DATE(activity_date)
)
SELECT 
    a1.activity_date,
    COUNT(DISTINCT a1.user_id) as total_users,
    COUNT(DISTINCT a2.user_id) as retained_users,
    ROUND(COUNT(DISTINCT a2.user_id) * 100.0 / COUNT(DISTINCT a1.user_id), 2) as retention_rate
FROM UserActivity a1
LEFT JOIN UserActivity a2 
    ON a1.user_id = a2.user_id 
    AND a2.activity_date = a1.activity_date + INTERVAL '1 day'
GROUP BY a1.activity_date
ORDER BY a1.activity_date;

-- Cohort retention (by registration week)
WITH UserCohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('week', registration_date) as cohort_week
    FROM users
),
UserActivity AS (
    SELECT 
        user_id,
        DATE_TRUNC('week', activity_date) as activity_week
    FROM user_logs
)
SELECT 
    c.cohort_week,
    COUNT(DISTINCT c.user_id) as cohort_size,
    COUNT(DISTINCT CASE WHEN a.activity_week = c.cohort_week THEN a.user_id END) as week_0,
    COUNT(DISTINCT CASE WHEN a.activity_week = c.cohort_week + INTERVAL '1 week' THEN a.user_id END) as week_1,
    COUNT(DISTINCT CASE WHEN a.activity_week = c.cohort_week + INTERVAL '2 week' THEN a.user_id END) as week_2
FROM UserCohorts c
LEFT JOIN UserActivity a ON c.user_id = a.user_id
GROUP BY c.cohort_week
ORDER BY c.cohort_week;
```

**PySpark:**
```python
# Day-over-day retention
from pyspark.sql.functions import date_add

user_activity = logs_df.select("user_id", col("activity_date").cast("date")).distinct()

retention = user_activity.alias("a1").join(
    user_activity.alias("a2"),
    (col("a1.user_id") == col("a2.user_id")) &
    (col("a2.activity_date") == date_add(col("a1.activity_date"), 1)),
    "left"
).groupBy("a1.activity_date").agg(
    countDistinct("a1.user_id").alias("total_users"),
    countDistinct("a2.user_id").alias("retained_users")
).withColumn("retention_rate", col("retained_users") / col("total_users") * 100)
```

---

### 13. Trip Cancellation Rate

**🎯 Scenario:** Calculate ride cancellation rate excluding banned users (Uber/Lyft analytics).

**💡 Solution:**
```sql
SELECT 
    request_date as Day,
    ROUND(
        SUM(CASE WHEN status LIKE 'cancelled%' THEN 1 ELSE 0 END) * 1.0 / 
        COUNT(*), 2
    ) as 'Cancellation Rate'
FROM trips t
JOIN users c ON t.client_id = c.id AND c.banned = 'No'
JOIN users d ON t.driver_id = d.id AND d.banned = 'No'
WHERE request_date BETWEEN '2023-10-01' AND '2023-10-03'
GROUP BY request_date;
```

---

### 14. Friends Recommendations (Graph Problem)

**🎯 Scenario:** Recommend friends-of-friends who aren't already connected (LinkedIn, Facebook).

**💡 Solution:**
```sql
WITH DirectFriends AS (
    SELECT user1_id as user_id, user2_id as friend_id FROM friendships
    UNION
    SELECT user2_id as user_id, user1_id as friend_id FROM friendships
)
SELECT DISTINCT 
    df1.user_id,
    df2.friend_id as recommended_friend
FROM DirectFriends df1
JOIN DirectFriends df2 ON df1.friend_id = df2.user_id
WHERE df1.user_id != df2.friend_id
  AND NOT EXISTS (
      SELECT 1 FROM DirectFriends df3 
      WHERE df3.user_id = df1.user_id AND df3.friend_id = df2.friend_id
  );
```

---

### 15. Pivot Table - Sales by Product and Month

**🎯 Scenario:** Convert rows to columns for reporting (Excel-style pivot).

**💡 Solution:**
```sql
SELECT 
    product_name,
    SUM(CASE WHEN MONTH(sale_date) = 1 THEN amount ELSE 0 END) as Jan,
    SUM(CASE WHEN MONTH(sale_date) = 2 THEN amount ELSE 0 END) as Feb,
    SUM(CASE WHEN MONTH(sale_date) = 3 THEN amount ELSE 0 END) as Mar,
    SUM(CASE WHEN MONTH(sale_date) = 4 THEN amount ELSE 0 END) as Apr,
    SUM(CASE WHEN MONTH(sale_date) = 5 THEN amount ELSE 0 END) as May,
    SUM(CASE WHEN MONTH(sale_date) = 6 THEN amount ELSE 0 END) as Jun
FROM sales
GROUP BY product_name;

-- Dynamic pivot in PostgreSQL
SELECT * FROM crosstab(
    'SELECT product_name, month, SUM(amount) FROM sales GROUP BY product_name, month ORDER BY 1,2',
    'SELECT DISTINCT month FROM sales ORDER BY 1'
) AS ct (product_name text, Jan numeric, Feb numeric, Mar numeric);
```

**PySpark:**
```python
df.groupBy("product_name").pivot("month").sum("amount")
```

---

### 16. Gap and Islands Problem

**🎯 Scenario:** Find consecutive sequences of active days (streak analysis).

**💡 Solution:**
```sql
WITH DateRanges AS (
    SELECT 
        user_id,
        activity_date,
        activity_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) * INTERVAL '1 day' as grp
    FROM user_activity
)
SELECT 
    user_id,
    MIN(activity_date) as start_date,
    MAX(activity_date) as end_date,
    COUNT(*) as consecutive_days
FROM DateRanges
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;
```

---

### 17. Self Join - Manager Hierarchy

**🎯 Scenario:** Show employee reporting hierarchy up to 3 levels.

**💡 Solution:**
```sql
SELECT 
    e1.name as Employee,
    e2.name as Manager,
    e3.name as Manager_of_Manager,
    e4.name as Top_Manager
FROM employees e1
LEFT JOIN employees e2 ON e1.manager_id = e2.id
LEFT JOIN employees e3 ON e2.manager_id = e3.id
LEFT JOIN employees e4 ON e3.manager_id = e4.id;

-- Recursive CTE for unlimited levels
WITH RECURSIVE EmployeeHierarchy AS (
    SELECT id, name, manager_id, 1 as level, name as path
    FROM employees WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.id, e.name, e.manager_id, eh.level + 1, eh.path || ' -> ' || e.name
    FROM employees e
    JOIN EmployeeHierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM EmployeeHierarchy ORDER BY level, path;
```

---

### 18. Find Top Spending Customers by Category

**💡 Solution:**
```sql
WITH CustomerSpending AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        p.category,
        SUM(o.amount) as total_spent,
        RANK() OVER (PARTITION BY p.category ORDER BY SUM(o.amount) DESC) as rank
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON o.product_id = p.id
    GROUP BY c.customer_id, c.customer_name, p.category
)
SELECT customer_id, customer_name, category, total_spent
FROM CustomerSpending
WHERE rank = 1;
```

---

### 19. Complex Date Logic - Active Subscriptions

**🎯 Scenario:** Count active subscriptions on each date.

**💡 Solution:**
```sql
SELECT 
    calendar_date,
    COUNT(DISTINCT user_id) as active_subscriptions
FROM (
    SELECT generate_series(
        '2024-01-01'::date, 
        '2024-12-31'::date, 
        '1 day'::interval
    )::date AS calendar_date
) dates
JOIN subscriptions s 
    ON calendar_date BETWEEN s.start_date AND s.end_date
GROUP BY calendar_date
ORDER BY calendar_date;
```

---

### 20. Moving Average (Last 7 Days)

**🎯 Scenario:** Smooth out daily fluctuations in metrics.

**💡 Solution:**
```sql
SELECT 
    date,
    sales,
    AVG(sales) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7days,
    AVG(sales) OVER (
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as moving_avg_30days
FROM daily_sales
ORDER BY date;
```

**PySpark:**
```python
from pyspark.sql.window import Window

window_7 = Window.orderBy("date").rowsBetween(-6, 0)
window_30 = Window.orderBy("date").rowsBetween(-29, 0)

df.withColumn("ma_7", avg("sales").over(window_7)) \
  .withColumn("ma_30", avg("sales").over(window_30))
```

---

## Data Engineering Patterns

### Pattern 1: String Manipulation
```sql
-- Extract domain from email
SELECT 
    email,
    SUBSTRING(email, POSITION('@' IN email) + 1) as domain
FROM users;

-- Extract first/last name
SELECT 
    SPLIT_PART(full_name, ' ', 1) as first_name,
    SPLIT_PART(full_name, ' ', 2) as last_name
FROM users;
```

### Pattern 2: CASE WHEN for Categorization
```sql
SELECT 
    customer_id,
    total_spent,
    CASE 
        WHEN total_spent > 10000 THEN 'Premium'
        WHEN total_spent > 5000 THEN 'Gold'
        WHEN total_spent > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_tier
FROM customer_spending;
```

### Pattern 3: Date Arithmetic
```sql
-- Age calculation
SELECT 
    DATEDIFF(year, birth_date, CURRENT_DATE) as age,
    DATE_TRUNC('month', order_date) as order_month,
    EXTRACT(DOW FROM order_date) as day_of_week,
    order_date + INTERVAL '30 days' as expected_delivery
FROM orders;
```

---

## Performance Optimization Tips

### 1. Indexing Strategy
```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_email ON users(email);
CREATE INDEX idx_order_date ON orders(order_date);
CREATE INDEX idx_composite ON orders(customer_id, order_date);

-- Covering index (includes all columns needed)
CREATE INDEX idx_covering ON orders(customer_id, order_date) INCLUDE (amount, status);
```

### 2. Partitioning
```sql
-- Range partitioning by date
CREATE TABLE orders (
    order_id INT,
    order_date DATE,
    amount DECIMAL
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_2024_q1 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
```

### 3. Query Optimization
```sql
-- Use EXISTS instead of IN for large subqueries
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- Use EXPLAIN ANALYZE to check query plans
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 123;
```

### 4. Avoid SELECT *
```sql
-- BAD: Retrieves all columns (slower, more memory)
SELECT * FROM large_table;

-- GOOD: Select only needed columns
SELECT customer_id, order_date, amount FROM orders;
```

### 5. Use CTEs for Readability
```sql
-- Easier to understand and maintain
WITH RecentOrders AS (
    SELECT * FROM orders WHERE order_date >= CURRENT_DATE - 30
),
CustomerSpending AS (
    SELECT customer_id, SUM(amount) as total FROM RecentOrders GROUP BY customer_id
)
SELECT * FROM CustomerSpending WHERE total > 1000;
```

---

## Interview Strategy

### Before the Interview

**📚 Study Plan:**
- **Week 1**: Easy + Medium questions (1-10)
- **Week 2**: Hard questions (11-20)
- **Week 3**: Mock interviews + system design

**🎯 Must-Know Concepts:**
- ✅ Window Functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)
- ✅ CTEs (Common Table Expressions)
- ✅ Self Joins
- ✅ GROUP BY / HAVING
- ✅ Date/Time functions
- ✅ String manipulation
- ✅ Subqueries vs JOINs

### During the Interview

**💡 Problem-Solving Approach:**

1. **Clarify Requirements**
   - "Do you want chronologically 3rd or 3rd largest?"
   - "How should I handle ties?"
   - "What about users with less than 3 records?"

2. **Think Aloud**
   - "I'm using ROW_NUMBER because..."
   - "I'll partition by user_id to rank within each user"
   - "Let me draw this join to visualize"

3. **Start Simple, Then Optimize**
   - Get a working solution first
   - Then optimize for performance
   - Mention trade-offs

4. **Use Specific Examples**
   - "In my last project, I used this pattern to..."
   - "I processed 500M records daily using this approach"
   - Include metrics (data volume, performance gains)

**🎯 Questions to Ask Them:**

**Technical:**
- What's the current tech stack and roadmap?
- What's the data volume and latency requirements?
- How does the team handle production issues?

**Cultural:**
- What does success look like in first 90 days?
- How is learning and development supported?
- What's the on-call rotation?

### For NAB Specifically

**Your Strengths:**
- ✅ 9+ years experience (exceeds 8+ requirement)
- ✅ Reasonable salary ask: 22 LPA → 30 LPA (36% increase)

**Preparation Focus:**
1. **SQL Beige Belt Test**: Practice 30+ problems from HackerRank/LeetCode
2. **Databricks Delta Lake**: Review hands-on experience
3. **AWS Services**: Be ready to discuss S3, EMR, Redshift, Lambda
4. **Financial Domain**: Prepare any fintech/banking examples

**Day-by-Day (10 Days Before):**
- **Days 1-3**: SQL intensive (30+ problems)
- **Days 4-5**: AWS + Azure hands-on
- **Days 6-7**: PySpark + system design
- **Days 8-9**: Mock interviews
- **Day 10**: Review + relax

---

## Additional Resources

### Online Platforms
- [LeetCode SQL](https://leetcode.com/problemset/database/)
- [HackerRank SQL](https://www.hackerrank.com/domains/sql)
- [StrataScratch](https://www.stratascratch.com/)
- [DataLemur](https://datalemur.com/)

### Books
- "SQL Performance Explained" - Markus Winand
- "Designing Data-Intensive Applications" - Martin Kleppmann

### PySpark Resources
- [Official PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Learning](https://www.databricks.com/learn)

---

## Quick Reference Card

### Window Functions Cheat Sheet
```sql
-- Ranking
ROW_NUMBER() OVER (ORDER BY salary DESC)  -- 1,2,3,4,5
RANK() OVER (ORDER BY salary DESC)        -- 1,2,2,4,5
DENSE_RANK() OVER (ORDER BY salary DESC)  -- 1,2,2,3,4

-- Offset
LAG(salary, 1) OVER (ORDER BY date)       -- Previous row
LEAD(salary, 1) OVER (ORDER BY date)      -- Next row

-- Aggregate
SUM(sales) OVER (ORDER BY date)                    -- Running total
AVG(sales) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  -- Moving avg
```

### Join Types
```sql
INNER JOIN      -- Only matching rows
LEFT JOIN       -- All from left + matching from right
RIGHT JOIN      -- All from right + matching from left
FULL OUTER JOIN -- All rows from both tables
CROSS JOIN      -- Cartesian product
LEFT ANTI JOIN  -- Rows in left NOT in right (Spark)
```

### Date Functions
```sql
CURRENT_DATE, CURRENT_TIMESTAMP
DATE_TRUNC('month', date)
EXTRACT(YEAR FROM date)
DATEDIFF(end_date, start_date)
date + INTERVAL '7 days'
```

---

**Good Luck with Your NAB Interview! 🚀**

*Remember: Practice makes perfect. Focus on understanding concepts, not memorizing solutions.*

---

## License
This guide is licensed under MIT License. Feel free to fork, modify, and share!

## Contributing
Found an error or have a better solution? Submit a PR or open an issue!

## Author
**Gurmukh Singh**  
Data Engineer | 9+ Years Experience  
Preparing for NAB Interview - February 2026

---

**⭐ Star this repo if it helped you!**
