# A Library for MySQL Vector Operations

## Overview
The `VectorTable` class is a PHP implementation designed to facilitate the storage and search of high-dimensional vectors in a MySQL database. This class stores normalized vectors as binary float arrays (VARBINARY) and quantized binary codes (VARBINARY), and uses a two-stage search algorithm for efficient similarity search.

### Computational Efficiency
The library stores only **normalized vectors** in the database, which provides computational efficiency, eliminating normalization overhead (cosine similarity is simply a dot product of normalized vectors).

### Search Performance
Vectors are binary quantized upon insertion into the database to optimize search speed and reranked to improve accuracy using a two-stage algorithm:
1. Fast filtering using Hamming distance on binary quantized codes
2. Precise re-ranking using cosine similarity (dot product) on normalized vectors

This library is suitable for datasets up to 1,000,000 vectors. For larger datasets, consider using a dedicated vector database such as [Qdrant](https://qdrant.tech/).

Search Benchmarks (384-dimensional vectors, MySQL 5.7):
Vectors | Time (seconds)
--------|---------------
100     | 0.0035
1000    | 0.0043
10000   | 0.0109
100000  | 0.0748
1000000 | 1.1343

## Storage Efficiency
Normalized vectors are stored as 32-bit IEEE-754 floats (float32) in little-endian order inside the `normalized_vector` VARBINARY column. Round-trip encoding/decoding to and from binary can introduce very small precision differences compared to 64-bit doubles; typical tolerances are around 1e-6 when comparing vectors after storage/retrieval. This precision is sufficient for cosine-similarity/dot-product ranking in typical embedding-based applications and allows significantly smaller storage and better performance than 64-bit doubles. Storing 4 bytes per dimension (instead of 8 bytes) allows 2x higher limit on dimensions and halves storage and network costs, which improves performance for insert, read, and re-ranking, while still maintaining sufficient accuracy for similarity search purposes.

Binary quantized codes are stored in the `binary_code` VARBINARY column. Storage requirements are 1 bit per dimension, so a 384-dimensional vector requires 48 bytes.

### High Dimension Support
Normalized vector storage uses VARBINARY(4 * dimension). MySQL's maximum VARBINARY length is 65,535 bytes, so the maximum supported dimension for float32 storage is floor(65,535 / 4) = 16,383. Binary quantized codes use VARBINARY(ceil(dimension/8)) for Stage 1 filtering. While this could theoretically support up to 524,280 bits, the effective limit is governed by the normalized vector storage (16,383 dimensions).

## Features
- Management of the database vector table.
- Support for multiple vector tables within a single database.
- Vector operations: insertion, deletion, retrieval, and search by cosine similarity.
- Support for high-dimensional vectors (up to 16,383 dimensions).
- Batch insert operations for efficient bulk vector storage.
- Batch delete operations to remove many vectors efficiently.
- Optional per-vector `metadata` stored as JSON included in retrieval and search results.

## Requirements
- PHP 7.2 or higher.
- MySQL 5.7.6 or higher / MariaDB 10.2.0 or higher.
- Required PHP extensions:
  - mysqli
  - json
  - ctype

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
> - Initialization will throw an exception if the target table already exists.
> - Ensure you use a unique base table name or clean up with deinitialize() before re-initializing.

#### Complete Initialization
The `initialize` method creates the vector table in the database:
```php
$vectorTable->initialize();
```

#### Initialization with typed metadata indexes (optional)
In order to use the metadata filtering capabilities efficiently, indexes on specific JSON paths need to be created.
Typed generated columns and their indexes are created by passing a map of JSON paths to SQL types:
```php
$vectorTable->initialize([
    '$.content_type' => 'ENUM("pdf","doc","txt","html")',
    '$.content_id'   => 'INT'
]);
```

The table schema includes:
- `id`: Auto-incrementing primary key
- `normalized_vector`: VARBINARY(4 * dimension) storing the L2-normalized vector in little-endian float32 format
- `binary_code`: VARBINARY column storing the binary quantized representation for fast filtering
- `metadata`: JSON column for optional per-vector metadata (NULL if absent)
- `metadata_<JSON_PATH_DERIVED_NAME>`: Generated columns for each JSON path in the map passed to initialize(), with the specified SQL type, used for indexed filtering

#### Cleanup and Deinitialization
The library provides comprehensive cleanup capabilities:
```php
// Clean up (drop) the table for this VectorTable instance
$vectorTable->deinitialize();
```

### Inserting and Managing Vectors
```php
// Insert a new vector
$vector = [0.1, 0.2, 0.3, /* ... */ 0.384];
$vectorId = $vectorTable->upsert($vector);

// Insert a new vector with metadata
$vector = [0.1, 0.2, 0.3 /* ... */ 0.384];
$metadata = ['content_type' => 'pdf', 'content_id' => 123, 'chunk_hash' => '123456'];
$vectorId = $vectorTable->upsert($vector, $metadata);

// Update an existing vector by ID
$updatedVector = [0.2, 0.1, 0.0, /* ... */ 0.123];
$updatedMetadata = ['content_type' => 'pdf', 'content_id' => 123, 'chunk_hash' => 'abcdef'];
$vectorTable->upsert($updatedVector, $updatedMetadata, $vectorId);

// Batch insert multiple vectors (each item contains a vector and optional metadata)
$items = [
    ['vector' => [0.1, 0.2, 0.3 /* ... */], 'metadata' => ['content_type' => 'pdf', 'content_id' => 123, 'chunk_hash' => 'aaa111']],
    ['vector' => [0.4, 0.5, 0.6 /* ... */], 'metadata' => ['content_type' => 'pdf', 'content_id' => 124, 'chunk_hash' => 'bbb222']],
];
$vectorTable->batchInsert($items);

// Delete a vector
$vectorTable->delete($vectorId);

// Batch delete vectors by id
$vectorTable->batchDelete([1, 2, 3]);

// Delete vectors by metadata (AND of equality on JSON path values)
$deletedCount = $vectorTable->deleteByMetadata(['$.content_type' => 'pdf', '$.content_id' => 123]);

// Truncate the vector table (remove all rows and reset AUTO_INCREMENT)
$vectorTable->truncate();
```

### Calculating Cosine Similarity
```php
// Calculate cosine similarity between two vectors
$similarity = $vectorTable->cosim($vector1, $vector2);
```

### Searching for Similar Vectors
Perform a search for vectors similar to a given vector using the two-stage cosine similarity algorithm. The `topN` parameter specifies the maximum number of similar vectors to return.

The library uses adaptive candidate selection for Stage‑1 (Hamming distance) filtering. By default, the candidate pool size is chosen based on the requested `topN` via:

- candidateMultiplier = max(3, min(20, ceil(100 / topN)))
- candidateLimit = topN * candidateMultiplier

This favors accuracy for small result sets (larger candidate pools) and efficiency for larger result sets (smaller multipliers). You can override this behavior by providing a custom multiplier.

```php
// Default adaptive candidate selection with default topN (10)
$similarVectors = $vectorTable->search($vector);

// Metadata pre-filter: only consider vectors where content_type='pdf' AND content_id=123
$similarVectors = $vectorTable->search(
    $vector,
    ['$.content_type' => 'pdf', '$.content_id' => 123],
    $topN=3
);

// Manual override: force a specific candidate multiplier
// Using 1 disables the adaptive expansion and uses exactly topN candidates in Stage‑1
$similarVectors = $vectorTable->search($vector, null, $topN, 1);

// Results include:
// - 'id': Vector ID
// - 'similarity': Cosine similarity score
// - 'metadata': Optional JSON metadata stored with the vector
foreach ($similarVectors as $result) {
    echo "Vector ID: {$result['id']}\n";
    echo "Similarity: {$result['similarity']}\n";
    echo 'Metadata: ' . json_encode($result['metadata']) . "\n";
}
```

### Additional Operations
```php
// Count total vectors in the table
$totalVectors = $vectorTable->count();

// Select specific vectors by ID (returns metadata if present)
$vectors = $vectorTable->select([1, 2, 3]);

// Select all vectors
$allVectors = $vectorTable->selectAll();

// Filter by metadata by JSON path values
$byType = $vectorTable->selectByMetadata(['$.content_type' => 'pdf', '$.content_id' => 456]);

// Get table name and dimension
$tableName = $vectorTable->getVectorTableName();
$dimension = $vectorTable->getDimension();
```

## Warning: Breaking changes in 3.x

This fork of the library (maxguru/mysql-vector) has breaking changes compared to the original 2.x version (allanpichardo/mysql-vector). Please review the relevant section before using this fork.

### 2.0.0 → 3.0.0

- Database schema overhaul
  - Removed columns: vector (JSON), normalized_vector (JSON), magnitude (DOUBLE)
  - Added columns: normalized_vector VARBINARY(4*dimension) with float32 LE encoding; binary_code VARBINARY; metadata JSON
  - Removed COSIM stored function; 3.x does not create or require any stored functions
  - Action: Drop the old 2.x table and recreate it with initialize(); existing 2.x tables are not compatible
  - Action: Drop the stored function in database

- initialize() signature and behavior
  - 2.x: initialize(bool $ifNotExists = true)
  - 3.0.0: initialize(array $metadataJsonPathIndexes = []) and fails if the table already exists
  - Action: Update calls to initialize() if you rely on $ifNotExists
  - Action: Update code to call initialize() only once if table doesn't exist; Use deinitialize() to drop before re-initializing
  - Action: Use deinitialize() to drop the table to clean up

- upsert() signature and parameter order
  - 2.x: upsert(array $vector, int $id = null)
  - 3.0.0: upsert(array $vector, ?array $metadata = null, ?int $id = null)
  - Note: Parameter order changed; add metadata as the second argument
  - Note: Vectors must match the table dimension (no automatic truncation)
  - Action: Update calls to upsert() to pass metadata (or null) before id
  - Action: Update code to handle new exceptions (e.g. InvalidArgumentException on dimension mismatch)

- batchInsert() input/return value
  - 2.x: batchInsert(array $vectors): array (returns inserted IDs)
  - 3.0.0: batchInsert(array $items): array (still returns inserted IDs); each item is ['vector' => array<float>, 'metadata' => array|null]
  - Action: Update calls to batchInsert() to pass array of items instead of vectors
  - Action: Update code to handle new exceptions (e.g. InvalidArgumentException on dimension mismatch)

- search() signature and result shape
  - 2.x: search(array $vector, int $n = 10) → results include id, vector, normalized_vector, magnitude, similarity
  - 3.0.0: search(array $vector, int $n = 10, ?int $candidateMultiplier = null)
    - Results now include id, similarity, metadata (vector contents are no longer returned)
    - Two‑stage algorithm retained; re‑ranking performed in PHP (no COSIM function)
  - Action: Update calls to search() to remove reliance on vector contents in results

- cosim() behavior and return type
  - 2.x: DB function COSIM on JSON vectors; returns float and could return NULL on mismatch
  - 3.0.0: Normalizes inputs and computes dot product in PHP with stricter validation (throws InvalidArgumentException on dimension mismatch)
  - Action: Update calls to cosim() to handle new exceptions and return type

- select()/selectAll() result shape
  - 2.x: Returned id, vector, normalized_vector, magnitude, binary_code
  - 3.0.0: Return id and metadata only
  - Action: Update calls to select()/selectAll() to remove reliance on vector contents in results

### 3.0.0 → 3.1.0

- search() signature expanded
  - 3.0.0: search(array $vector, int $n = 10, ?int $candidateMultiplier = null)
  - 3.1.0: search(array $vector, ?array $conditions = null, int $n = 10, ?int $candidateMultiplier = null)
  - Action: If you previously called search($v, 5), update to search($v, null, 5) to pass topN as the third argument.

- batchInsert() return type changed
  - 3.0.0: batchInsert(array $items): array (returned inserted IDs)
  - 3.1.0: batchInsert(array $items): void (no longer returns IDs)
  - Note: IDs are no longer returned by batchInsert() to avoid overhead and complexity
  - Action: Update calls to batchInsert() to remove reliance on return value; suggested alternative: use upsert() or store metadata to distinguish vectors

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

### PHP CompatInfo (compatibility analysis)

Use PHP CompatInfo to run an analysis of the library:

```bash
composer compatinfo
```

- Initializes the php-compatinfo database at `vendor/.cache/php-compatinfo/compatinfo-db.sqlite` (first run only)
- Runs the analyser on `src`

If you need to rebuild the php-compatinfo database, use:

```bash
composer compatinfo:rebuild
```

### Roadmap
The following features are planned for future releases:

1) MariaDB 11.7 VECTOR INDEX Support
   - What: Integrate native vector indexing (HNSW) using [MariaDB 11.7's VECTOR INDEX feature](https://mariadb.com/docs/server/reference/sql-structure/vectors/vector-overview).
   - Why: Leverages database-side graph search for faster similarity queries. The current two‑stage (Hamming filter + cosine re‑rank) path will remain as a compatible fallback for servers without native vector indexing.

2) Schema Migration System
   - What: Provide an automated, versioned migration system to update existing table schemas (columns, generated indexes) when upgrading library versions.
   - Why: Enables seamless upgrades across library versions without manual steps or data loss, improving reliability and maintainability for production deployments.

3) Enhanced Metadata Filtering
   - What: Expand metadata querying with composite index support and a richer, type‑aware query builder for complex filters (e.g., nested AND/OR, IN/LIKE, numeric ranges).
   - Why: Delivers faster, more expressive filtering over JSON metadata, improving relevance and performance for applications with nuanced search requirements.


### Sponsors
If you find this library useful and would like to see the roadmap delivered faster, please consider sponsoring the developers of the library. Your support is greatly appreciated.

Alex R.:
- GitHub Sponsors: https://github.com/sponsors/maxguru
- Liberapay: https://liberapay.com/maxguru

## License
MIT License
