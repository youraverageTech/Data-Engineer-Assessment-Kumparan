-- Melakukan truncate pada tabel 'articles' dan 'authors' untuk menghapus data lama dan mereset ID agar tidak terjadi konflik saat melakukan insert data baru.
TRUNCATE TABLE articles RESTART IDENTITY CASCADE;
TRUNCATE TABLE authors RESTART IDENTITY CASCADE;

-- melakukan insert data dummy ke dalam table authors
INSERT INTO authors (id, name, email, created_at, updated_at) VALUES
(1, 'Aditya Pratama', 'aditya.pratama@techcorp.com', '2026-05-01 08:00:00', '2026-05-01 08:00:00'),
(2, 'Budi Santoso', 'budi.santoso@techcorp.com', '2026-05-01 09:30:00', '2026-05-01 09:30:00'),
(3, 'Citra Lestari', 'citra.lestari@techcorp.com', '2026-05-02 10:15:00', '2026-05-02 10:15:00'),
(4, 'Dewi Sartika', 'dewi.sartika@techcorp.com', '2026-05-03 11:00:00', '2026-05-03 11:00:00'),
(5, 'Eko Wijaya', 'eko.wijaya@techcorp.com', '2026-05-04 14:20:00', '2026-05-04 14:20:00'),
(6, 'Farhan Halim', 'farhan.halim@techcorp.com', '2026-05-05 09:00:00', '2026-05-05 09:00:00'),
(7, 'Gita Wirjawan', 'gita.wirjawan@techcorp.com', '2026-05-06 16:45:00', '2026-05-06 16:45:00'),
(8, 'Hadi Wibowo', 'hadi.wibowo@techcorp.com', '2026-05-07 13:10:00', '2026-05-07 13:10:00'),
(9, 'Indah Permata', 'indah.permata@techcorp.com', '2026-05-08 10:00:00', '2026-05-08 10:00:00'),
(10, 'Joko Susilo', 'joko.susilo@techcorp.com', '2026-05-09 15:30:00', '2026-05-09 15:30:00');

-- melakukan insert data dummy ke dalam table articles
INSERT INTO articles (id, title, content, published_at, author_id, created_at, updated_at, deleted_at) VALUES
-- Author 1 (Aditya Pratama)
(1, 'Introduction to Data Engineering', 'Data engineering is the practice of designing and building systems for collecting, storing, and analyzing data at scale.', '2026-05-01 10:00:00', 1, '2026-05-01 08:30:00', '2026-05-01 10:00:00', NULL),
(11, 'Modern Data Stack in 2026', 'The Modern Data Stack continues to evolve. We look into the consolidation of storage, metadata layers, and the integration of semantic layers.', '2026-05-12 11:00:00', 1, '2026-05-11 09:00:00', '2026-05-12 11:00:00', NULL),
(21, 'Building Real-time Data Pipelines with Apache Kafka', 'This post covers the core components of Apache Kafka and how to construct high-throughput streaming architectures for production.', NULL, 1, '2026-05-18 14:00:00', '2026-05-18 14:00:00', NULL), -- Draft

-- Author 2 (Budi Santoso)
(2, 'Getting Started with PostgreSQL', 'PostgreSQL is a powerful, open-source object-relational database system with over 35 years of active development.', '2026-05-02 09:00:00', 2, '2026-05-02 08:00:00', '2026-05-02 09:00:00', NULL),
(12, 'Optimizing PostgreSQL Queries for Large Datasets', 'In this advanced guide, we explore indexes, partitioning, query planner anatomy, and analyzing slow queries using EXPLAIN ANALYZE.', '2026-05-13 14:30:00', 2, '2026-05-12 10:00:00', '2026-05-13 14:30:00', NULL),
(22, 'Understanding Database Indexes', 'This post explains how B-Tree, Hash, and GIN indexes function inside a relational database to speed up operations.', '2026-05-05 10:00:00', 2, '2026-05-05 09:15:00', '2026-05-18 16:00:00', '2026-05-18 16:00:00'), -- Soft-deleted

-- Author 3 (Citra Lestari)
(3, 'Introduction to Apache Airflow', 'Apache Airflow is a platform to programmatically author, schedule, and monitor workflows as Directed Acyclic Graphs (DAGs).', '2026-05-03 14:00:00', 3, '2026-05-03 12:00:00', '2026-05-03 14:00:00', NULL),
(13, 'Best Practices for Writing Clean DAGs', 'Learn about modularizing your code, avoiding top-level database queries inside your DAG files, and using TaskFlow API.', '2026-05-14 10:00:00', 3, '2026-05-13 11:00:00', '2026-05-14 10:00:00', NULL),
(23, 'Advanced Airflow Scheduling & Dynamic Tasks', 'Dynamic task mapping allows you to generate tasks dynamically at runtime. Here is a deep dive with examples.', NULL, 3, '2026-05-19 09:00:00', '2026-05-19 09:00:00', NULL), -- Draft

-- Author 4 (Dewi Sartika)
(4, 'Why Python is the King of Data Science', 'Python offers excellent readability, extensive libraries, and massive community support, making it the top choice for data practitioners.', '2026-05-04 11:00:00', 4, '2026-05-04 10:00:00', '2026-05-04 11:00:00', NULL),
(14, 'Deep Dive into Pandas 2.0', 'Pandas 2.0 introduces Apache Arrow backend integration. Let''s explore how it improves performance and memory footprint.', '2026-05-15 09:00:00', 4, '2026-05-14 15:00:00', '2026-05-15 09:00:00', NULL),
(24, 'Outdated Data Analysis Tools', 'A historical overview of legacy tools that have been superseded by modern python libraries and distributed SQL engines.', '2026-05-05 11:00:00', 4, '2026-05-05 10:00:00', '2026-05-19 10:00:00', '2026-05-19 10:00:00'), -- Soft-deleted

-- Author 5 (Eko Wijaya)
(5, 'Docker for Developers: A Hands-On Guide', 'Learn how to containerize your applications, configure multi-container environments using Docker Compose, and minimize image sizes.', '2026-05-05 15:00:00', 5, '2026-05-05 13:00:00', '2026-05-05 15:00:00', NULL),
(15, 'Kubernetes Basics for Data Engineers', 'This article covers pods, deployments, services, and how stateful applications are deployed on a Kubernetes cluster.', '2026-05-15 16:00:00', 5, '2026-05-15 11:00:00', '2026-05-15 16:00:00', NULL),
(25, 'Scaling Containerized Applications Under Load', 'In this post, we discuss Horizontal Pod Autoscaler (HPA) and node provisioning mechanisms to handle peak web traffic.', NULL, 5, '2026-05-19 13:00:00', '2026-05-19 13:00:00', NULL), -- Draft

-- Author 6 (Farhan Halim)
(6, 'The Rise of Large Language Models', 'We explore the history, transformer architecture, and rapid deployment of LLMs in solving commercial software engineering tasks.', '2026-05-06 10:00:00', 6, '2026-05-06 09:00:00', '2026-05-06 10:00:00', NULL),
(16, 'Fine-Tuning Open Source LLMs', 'A step-by-step guide to fine-tuning models like LLaMA and Mistral using LoRA and QLoRA techniques on custom hardware.', '2026-05-16 11:00:00', 6, '2026-05-15 14:00:00', '2026-05-16 11:00:00', NULL),
(26, 'Obsolete AI Models in 2026', 'Reflecting on neural network architectures from the early 2010s that are no longer widely utilized in today''s generative pipelines.', '2026-05-07 10:00:00', 6, '2026-05-07 08:30:00', '2026-05-19 12:00:00', '2026-05-19 12:00:00'), -- Soft-deleted

-- Author 7 (Gita Wirjawan)
(7, 'Economic Trends in Southeast Asia''s Tech Industry', 'This article discusses market cycles, investment funding shifts, and growing digital economies in developing markets.', '2026-05-07 16:00:00', 7, '2026-05-07 14:00:00', '2026-05-07 16:00:00', NULL),
(17, 'The Future of Remote Work in Indonesia', 'With digital nomad hotspots and tech companies adopting hybrid strategies, what does the working landscape look like in 2026?', '2026-05-17 14:00:00', 7, '2026-05-16 10:00:00', '2026-05-17 14:00:00', NULL),
(27, 'Capital Venture Outlook 2027', 'An early forecast of tech trends, investment sizes, and key sectors positioned for substantial growth next year.', NULL, 7, '2026-05-20 09:00:00', '2026-05-20 09:00:00', NULL), -- Draft

-- Author 8 (Hadi Wibowo)
(8, 'Introduction to Snowflake DWH', 'Snowflake provides a software-as-a-service cloud-based data warehouse solution that separates compute from storage for cost-efficiency.', '2026-05-08 09:00:00', 8, '2026-05-08 08:00:00', '2026-05-08 09:00:00', NULL),
(18, 'Snowflake vs Databricks: A Comprehensive Comparison', 'We break down the architectural differences, SQL compliance, ETL support, machine learning integration, and performance costs.', '2026-05-18 10:00:00', 8, '2026-05-17 11:00:00', '2026-05-18 10:00:00', NULL),
(28, 'Advanced Snowflake Snowpark Guide', 'Utilizing Python and Java within Snowflake to construct custom processing pipelines directly on the warehouse engine.', NULL, 8, '2026-05-20 10:30:00', '2026-05-20 10:30:00', NULL), -- Draft

-- Author 9 (Indah Permata)
(9, 'UI/UX Best Practices for Dashboard Design', 'An engaging dashboard is intuitive, clean, and highlights primary business metrics without overwhelming the active user.', '2026-05-09 13:00:00', 9, '2026-05-09 11:00:00', '2026-05-09 13:00:00', NULL),
(19, 'Visualizing Complex Data: Tips & Tricks', 'Choose the right charts. Avoid pie charts for many categories, utilize heatmaps, and implement clean tooltips.', '2026-05-19 15:00:00', 9, '2026-05-18 16:00:00', '2026-05-19 15:00:00', NULL),
(29, 'Archived UI Design Standards', 'Reviewing legacy web interface guidelines and styling conventions from the early 2000s desktop era.', '2026-05-10 09:00:00', 9, '2026-05-09 15:00:00', '2026-05-20 11:00:00', '2026-05-20 11:00:00'), -- Soft-deleted

-- Author 10 (Joko Susilo)
(10, 'Healthy Work-Life Balance for Software Engineers', 'Practical strategies for managing screen time, avoiding overtime culture, setting boundaries, and establishing proper ergonomic setups.', '2026-05-10 10:00:00', 10, '2026-05-10 09:00:00', '2026-05-10 10:00:00', NULL),
(20, 'Mental Health in Tech: Let''s Talk About Burnout', 'Burnout is highly prevalent in software and data engineering. We discuss symptoms, structural causes, and therapeutic recovery.', '2026-05-20 09:00:00', 10, '2026-05-19 14:00:00', '2026-05-20 09:00:00', NULL),
(30, 'Temporary Article for Testing', 'A dummy post created to test database connection bandwidth, high volume loads, and fast deletion performance.', '2026-05-11 11:00:00', 10, '2026-05-11 10:00:00', '2026-05-20 15:30:00', '2026-05-20 15:30:00'); -- Soft-deleted

-- melakukan resert sequence pada table authors dan articles agar ID yang dihasilkan tidak terjadi konflik dengan data yang sudah ada
SELECT setval(pg_get_serial_sequence('authors', 'id'), COALESCE(MAX(id), 1)) FROM authors;
SELECT setval(pg_get_serial_sequence('articles', 'id'), COALESCE(MAX(id), 1)) FROM articles;