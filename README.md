# A Library for MySQL Vector Operations

## Overview
The `VectorTable` class is a PHP implementation designed to facilitate the storage and search of high-dimensional vectors in a MySQL database. This class stores normalized vectors as binary float arrays (VARBINARY) and quantized binary codes (VARBINARY), and uses a two-stage search algorithm for efficient similarity search.

### Computational Efficiency
The library stores only **normalized vectors** in the database, which provides computational efficiency, eliminating normalization overhead (cosine similarity is simply a dot product of normalized vectors).

### Search Performance
Vectors are binary quantized upon insertion into the database to optimize search speed and reranked to improve accuracy using a two-stage algorithm:
1. **Stage 1**: Fast filtering using Hamming distance on binary quantized codes
2. **Stage 2**: Precise re-ranking using cosine similarity (dot product) on normalized vectors

This library is suitable for datasets up to 1,000,000 vectors. For larger datasets, consider using a dedicated vector database such as [Qdrant](https://qdrant.tech/).

Search Benchmarks (384-dimensional vectors):
Vectors | Time (seconds)
--------|---------------
100     | 0.02
1000    | 0.02
10000   | 0.03
100000  | 0.06
1000000 | 0.48

## Storage Efficiency
Normalized vectors are stored as 32-bit IEEE-754 floats (float32) in little-endian order inside the `normalized_vector` VARBINARY column. Round-trip encoding/decoding to and from binary can introduce very small precision differences compared to 64-bit doubles; typical tolerances are around 1e-6 when comparing vectors after storage/retrieval. This precision is sufficient for cosine-similarity/dot-product ranking in typical embedding-based applications and allows significantly smaller storage and better performance than 64-bit doubles. Storing 4 bytes per dimension (instead of 8 bytes) allows 2x higher limit on dimensions and halves storage and network costs, which improves performance for insert, read, and re-ranking, while still maintaining sufficient accuracy for similarity search purposes.

Binary quantized codes are stored in the `binary_code` VARBINARY column. Storage requirements are 1 bit per dimension, so a 384-dimensional vector requires 48 bytes.

### High Dimension Support
Normalized vector storage uses VARBINARY(4 * dimension). MySQL's maximum VARBINARY length is 65,535 bytes, so the maximum supported dimension for float32 storage is floor(65,535 / 4) = 16,383. Binary quantized codes use VARBINARY(ceil(dimension/8)) for Stage 1 filtering. While this could theoretically support up to 524,280 bits, the effective limit is governed by the normalized vector storage (16,383 dimensions).

## Features
- Management of database tables.
- Support for multiple vector tables within a single database.
- Vector operations: insertion, deletion, retrieval, and search by cosine similarity.
- Support for high-dimensional vectors (up to 16,383 dimensions for normalized vector storage).
- Batch insert operations for efficient bulk vector storage.

## Requirements
- PHP 8.0 or higher.
- MySQL 5.7 or higher.
- A MySQLi extension for PHP.

## Installation
1. Ensure that PHP and MySQL are installed and properly configured on your system.
2. Install the library using [Composer](https://getcomposer.org/).

   ```bash
   composer require allanpichardo/mysql-vector
   ```

## Usage

### Initializing the Vector Table
Import the `VectorTable` class and create a new instance using the MySQLi connection, table name, and vector dimension.
```php
use MHz\MysqlVector\VectorTable;


$mysqli = new mysqli("hostname", "username", "password", "database");
$tableName = "my_vector_table";
$dimension = 384;
$engine = 'InnoDB';

$vectorTable = new VectorTable($mysqli, $tableName, $dimension, $engine);
```

### Setting Up the Vector Table in MySQL
The library provides flexible initialization options for different use cases:

> Important
> - Initialization methods will throw an exception if the target tables already exists.
> - Ensure you use a unique base table name or clean up with deinitializeTables() before re-initializing.

#### Complete Initialization
The `initialize` method creates the vector table:
```php
$vectorTable->initialize();
```

#### Granular Initialization
For more control, you can initialize tables separately:
```php
// Initialize tables for multiple vector tables
$vectorTable1->initializeTables();
$vectorTable2->initializeTables();
```

The table schema includes:
- `id`: Auto-incrementing primary key
- `normalized_vector`: VARBINARY(4 * dimension) storing the L2-normalized vector in little-endian float32 format
- `binary_code`: VARBINARY column storing the binary quantized representation for fast filtering

#### Cleanup and Deinitialization
The library provides comprehensive cleanup capabilities:
```php
// Clean up tables for this VectorTable instance
$vectorTable->deinitializeTables();

// Complete cleanup (tables)
$vectorTable->deinitialize();
```

### Inserting and Managing Vectors
```php
// Insert a new vector
$vector = [0.1, 0.2, 0.3, ..., 0.384];
$vectorId = $vectorTable->upsert($vector);

// Update an existing vector
$vectorTable->upsert($vector, $vectorId);

// Batch insert multiple vectors for better performance
$vectors = [
    [0.1, 0.2, 0.3, ...],
    [0.4, 0.5, 0.6, ...],
    // ... more vectors
];
$vectorIds = $vectorTable->batchInsert($vectors);

// Delete a vector
$vectorTable->delete($vectorId);
```

### Calculating Cosine Similarity
```php
// Calculate cosine similarity between two vectors
$similarity = $vectorTable->cosim($vector1, $vector2);
```

### Searching for Similar Vectors
Perform a search for vectors similar to a given vector using the two-stage cosine similarity algorithm. The `topN` parameter specifies the maximum number of similar vectors to return.
```php
// Find vectors similar to a given vector
$similarVectors = $vectorTable->search($vector, $topN);

// Results include:
// - 'id': Vector ID
// - 'similarity': Cosine similarity score
foreach ($similarVectors as $result) {
    echo "Vector ID: {$result['id']}, Similarity: {$result['similarity']}\n";
}
```

### Additional Operations
```php
// Count total vectors in the table
$totalVectors = $vectorTable->count();

// Select specific vectors by ID
$vectors = $vectorTable->select([1, 2, 3]);

// Select all vectors
$allVectors = $vectorTable->selectAll();

// Get table name and dimension
$tableName = $vectorTable->getVectorTableName();
$dimension = $vectorTable->getDimension();
```


## Contributions
Contributions to this project are welcome. Please ensure that your code adheres to the existing coding standards and includes appropriate tests.

## Development
This project uses DDEV, a Docker-based development environment. To get started, install DDEV and run the following commands:

```bash
ddev start
ddev composer install
```

To run the tests, use the following command:

```bash
ddev composer test
```

## License
MIT License