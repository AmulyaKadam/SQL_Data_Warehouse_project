# 🏗️ SQL Data Warehouse Project

This project demonstrates the **end-to-end design and implementation** of a modern **Data Warehouse architecture** using a **multi-layered approach** — Bronze, Silver, and Gold layers. It involves data ingestion, transformation, and preparation for analytics and business intelligence consumption.

---

## 📘 Table of Contents

- [Detailed Overview](#detailed-overview)
- [🚀 Project Requirements](#-project-requirements)
- [Architecture](#architecture)
- [Data Flow (Data Lineage)](#data-flow-data-lineage)
- [Technologies Used](#technologies-used)
- [Folder Structure](#folder-structure)
- [How to Use](#how-to-use)
- [Future Enhancements](#future-enhancements)
- [Contact](#contact)

---

## 🧩 Detailed Overview

The **SQL Data Warehouse Project** is a comprehensive demonstration of how to design and implement a **modern, enterprise-grade data warehouse** using **Microsoft SQL Server**.  

The goal of the project is to **consolidate, cleanse, and transform** data from multiple business systems — specifically **ERP** and **CRM** — into a unified analytical model that enables business users to gain insights and make data-driven decisions.

This project showcases the full **ETL (Extract, Transform, Load)** lifecycle:  
1. Extracting data from heterogeneous sources.  
2. Cleansing and transforming it to standardize and improve data quality.  
3. Loading it into structured layers (Bronze, Silver, Gold) following the **Medallion Architecture pattern**.  

The output is a **business-ready data warehouse** that supports BI dashboards, SQL analytical queries, and machine learning models.  

This repository serves as a reference for **data engineers**, **analysts**, and **students** who wish to learn how to build scalable and maintainable data warehouse pipelines using SQL.

---

## 🚀 Project Requirements

### 🧠 Objective
Develop a **modern data warehouse** using **SQL Server** to consolidate sales data, enabling analytical reporting and informed decision-making.

### 📋 Specifications

- **Data Sources:** Import data from two source systems (**ERP** and **CRM**) provided as CSV files.  
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis.  
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.  
- **Scope:** Focus on the latest dataset only; historization of data is not required.  
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams.  

---

## 🏗️ Architecture

![High Level Architecture](./Data_architecture.drawio.png)

---

## 🔄 Data Flow (Data Lineage)

![Data Flow Diagram](./Flow_diagram.drawio.png)

---

## ⚙️ Technologies Used

| Category | Tools/Technologies |
|-----------|--------------------|
| **Database** | Microsoft SQL Server / Azure SQL |
| **Data Processing** | SQL Stored Procedures |
| **ETL** | SQL-based Transformation Logic |
| **Visualization** | Power BI / Tableau |
| **Data Sources** | CRM & ERP CSV files |

---

## 📁 Folder Structure

```
SQL_Data_Warehouse_project/
│
├── /Data/
│   ├── CRM/
│   ├── ERP/
│
├── /SQL_Scripts/
│   ├── bronze_layer.sql
│   ├── silver_layer.sql
│   ├── gold_layer.sql
│
├── /Images/
│   ├── Data_architecture.drawio.png
│   ├── Flow_diagram.drawio.png
│
├── README.md
└── LICENSE
```

---

## 🚀 How to Use

1. **Clone the repository**
   ```bash
   git clone https://github.com/AmulyaKadam/SQL_Data_Warehouse_project.git
   cd SQL_Data_Warehouse_project
   ```

2. **Set up your SQL Server**
   - Create a new database named `DataWarehouse`
   - Execute SQL scripts from `/SQL_Scripts` folder in order:
     - Bronze → Silver → Gold

3. **Load Source Data**
   - Place CSV files in `/Data/CRM` and `/Data/ERP` folders  
   - Run ingestion scripts to populate the Bronze layer

4. **Transform Data**
   - Execute stored procedures to process data into Silver and Gold layers

5. **Visualize Results**
   - Connect BI tools (like Power BI) to the Gold Layer views
   - Run analytical SQL queries or use the data for ML models

---

## 🔮 Future Enhancements

- Automate ETL process using **Azure Data Factory / Airflow**
- Add **incremental data loads**
- Implement **data quality monitoring dashboards**
- Introduce **data versioning** and **change data capture (CDC)**

---

## 👤 Contact

**Author:** Amulya Kadam  
📧 **Email:** your-email@example.com  
💼 **GitHub:** [AmulyaKadam](https://github.com/AmulyaKadam)  
🔗 **LinkedIn:** [linkedin.com/in/AmulyaKadam](#)

---

### ⭐ If you found this project helpful, please star the repository!
