<?php

namespace MHz\MysqlVector;

class VectorTable
{
    private string $name;
    private int $dimension;
    private string $engine;
    private \mysqli $mysqli;
    private ?array $metadataIndexMap = null;

    // Maximum supported vector dimensions, currently limited by VARBINARY storage
    // `normalized_vector` column uses VARBINARY(4 * dimension); VARBINARY max length in MySQL is 65,535 bytes
    // maximum supported dimensions for float32 storage = floor(65535 bytes / 4 bytes per float32) = 16383
    public const MAX_DIMENSIONS = 16383;

    // Safe default for utf8mb4 in MySQL 5.7: 191 characters (191*4=764 bytes < 767-byte limit)
    private const INDEX_PREFIX_LENGTH = 191;

    /**
     * Instantiate a new VectorTable object.
     * @param \mysqli $mysqli The mysqli connection
     * @param string $name Name of the table.
     * @param int $dimension Dimension of the vectors.
     * @param string $engine The storage engine to use for the table
     * @throws \InvalidArgumentException If dimension exceeds maximum supported value
     */
    public function __construct(\mysqli $mysqli, string $name, int $dimension = 384, string $engine = 'InnoDB')
    {
        if ($dimension <= 0) {
            throw new \InvalidArgumentException("Dimension must be a positive integer, got $dimension");
        }

        if ($dimension > self::MAX_DIMENSIONS) {
            throw new \InvalidArgumentException("Maximum supported dimension is " . self::MAX_DIMENSIONS . ", got $dimension");
        }

        $this->mysqli = $mysqli;
        $this->name = $name;
        $this->dimension = $dimension;
        $this->engine = $engine;
    }

    /**
     * Escape MySQL identifier using backticks
     *
     * @param string $identifier The identifier to escape
     * @return string The escaped identifier
     */
    private function escapeIdentifier(string $identifier): string
    {
        // For identifiers, escape backticks by doubling them, then wrap in backticks
        $escaped = str_replace('`', '``', $identifier);
        return "`$escaped`";
    }

    public function getVectorTableName(): string
    {
        return $this->name . '_vectors';
    }

    public function getDimension(): int
    {
        return $this->dimension;
    }

    /**
     * Determine support for virtual generated columns with indexes
     */
    private function supportsVirtualGeneratedColumns(): bool
    {
        $info = $this->mysqli->server_info;
        $isMaria = stripos($info, 'mariadb') !== false;
        if ($isMaria) {
            // Extract version before -MariaDB
            if (preg_match('/(\d+\.\d+\.\d+)/i', $info, $m)) {
                return version_compare($m[1], '10.2.0', '>=');
            }
            return false;
        }
        // MySQL
        if (preg_match('/(\d+\.\d+\.\d+)/', $info, $m)) {
            return version_compare($m[1], '5.7.6', '>=');
        }
        return false;
    }

    /**
     * Validate JSON path format (e.g., $.a.b.c)
     * Returns the same path if valid, otherwise throws InvalidArgumentException
     */
    private function validateJsonPath(string $path): string
    {
        $path = trim($path);
        if ($path === '') {
            throw new \InvalidArgumentException('JSON path cannot be empty');
        }
        if ($path[0] !== '$') {
            throw new \InvalidArgumentException('JSON path must start with $.');
        }
        // Strict allowed pattern: $.identifier(.identifier)*
        if (!preg_match('/^\$\.(?:[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:[A-Za-z_][A-Za-z0-9_]*))*$/', $path)) {
            throw new \InvalidArgumentException('Invalid JSON path format: ' . $path);
        }
        return $path;
    }

    /**
     * Map an SQL column type to a JSON primitive type: string|integer|float|boolean|null
     * DECIMAL/NUMERIC are treated as float; DATETIME/TIMESTAMP as integer (unix epoch);
     * DATE as string; TINYINT(1) as boolean; other TINYINT as integer.
     */
    private function detectPrimitiveType(string $sqlType): string
    {
        $t = strtoupper(trim($sqlType));
        // Extract base type token
        $base = preg_match('/^([A-Z]+)\b/', $t, $m) ? $m[1] : $t;
        switch ($base) {
            case 'TINYINT':
                // BOOLEAN/TINYINT(1) map to boolean; other TINYINT map to integer
                if (preg_match('/TINYINT\s*\(\s*1\s*\)/', $t)) { return 'boolean'; }
                return 'integer';
            case 'BOOLEAN':
                return 'boolean';
            case 'SMALLINT':
            case 'MEDIUMINT':
            case 'INT':
            case 'INTEGER':
            case 'BIGINT':
            case 'DATETIME':
            case 'TIMESTAMP':
                return 'integer';
            case 'DECIMAL':
            case 'NUMERIC':
            case 'FLOAT':
            case 'DOUBLE':
            case 'REAL':
                return 'float';
            default:
                return 'string';
        }
    }

    /**
     * Determine if the SQL type requires an index prefix length when creating an index.
     * Returns true for TEXT/BLOB families and very large VARCHAR columns where full-length index may exceed limits.
     */
    private function requiresIndexPrefixLength(string $sqlType): bool
    {
        $t = strtoupper(trim($sqlType));
        // TEXT/BLOB families always require a prefix length
        if (preg_match('/\b(TINYTEXT|TEXT|MEDIUMTEXT|LONGTEXT|TINYBLOB|BLOB|MEDIUMBLOB|LONGBLOB)\b/', $t)) {
            return true;
        }
        // Heuristic for large VARCHAR: if declared length > 191 (utf8mb4 safe default)
        if (preg_match('/\bVARCHAR\s*\(\s*(\d+)\s*\)/', $t, $m)) {
            $len = (int)$m[1];
            return $len > self::INDEX_PREFIX_LENGTH;
        }
        return false;
    }

    /**
     * Get the metadata index map for this table, lazily detecting existing generated columns
     */
    private function getMetadataIndexMap(): array
    {
        if ($this->metadataIndexMap !== null) {
            return $this->metadataIndexMap;
        }

        $this->metadataIndexMap = [];

        $tableName = $this->getVectorTableName();
        $sql = "SELECT COLUMN_NAME, COLUMN_TYPE, GENERATION_EXPRESSION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME LIKE 'metadata\\_%'";
        $stmt = $this->mysqli->prepare($sql);
        if ($stmt) {
            $stmt->bind_param('s', $tableName);
            if ($stmt->execute()) {
                $stmt->bind_result($colName, $colType, $genExpr);
                while ($stmt->fetch()) {
                    $path = null;
                    if (is_string($genExpr)) {
                        // Extract JSON path from generated column expression like
                        // JSON_EXTRACT(`metadata`, '$.content_type') and
                        // JSON_UNQUOTE(JSON_EXTRACT(metadata, _utf8mb4'$.price'))
                        if (preg_match("/JSON_EXTRACT\\s*\\(\\s*`?metadata`?\\s*,\\s*(?:_[A-Za-z0-9]+)?\\s*'([^']+)'\\s*\\)/i", $genExpr, $m)) {
                            $path = $m[1];
                        }
                    }
                    if ($path !== null) {
                        $this->metadataIndexMap[$path] = [
                            'column' => $colName,
                            'primitive_type' => $this->detectPrimitiveType($colType),
                        ];
                    }
                }
            }
            $stmt->close();
        }

        return $this->metadataIndexMap;
    }

    /**
     * Convert an n-dimensional vector into an n-bit binary code using optimized chunking
     * @param array $vector Input vector
     * @return string Hexadecimal representation of binary quantized vector
     */
    public function vectorToHex(array $vector): string {
        $bytes = [];
        $chunks = array_chunk($vector, 8);

        foreach ($chunks as $chunk) {
            $byte = 0;
            foreach ($chunk as $i => $val) {
                if ($val > 0) {
                    $byte |= (1 << $i);
                }
            }
            $bytes[] = $byte;
        }

        return bin2hex(pack('C*', ...$bytes));
    }

    /**
     * Encode a PHP float vector to a little-endian float32 binary blob.
     * Uses pack('g', ...) to enforce LE regardless of platform endianness.
     */
    public function vectorToBlob(array $vector): string {
        $bin = '';
        foreach ($vector as $v) {
            $bin .= pack('g', (float)$v);
        }
        return $bin;
    }

    /**
     * Decode a little-endian float32 binary blob to a PHP float array.
     * Uses unpack('g*', ...) for single-pass decoding.
     */
    public function blobToVector(string $blob): array {
        // array_values to reindex from 0
        return array_values(unpack('g*', $blob));
    }

    /**
     * Initialize the vector table for this instance
     * Optionally create generated columns and indexes for JSON metadata paths
     * Fails if the table has already been created
     * @param array $metadataJsonPathIndexes Map of JSON paths to SQL types for generated columns and indexes.
     *        Example: ['$.content_type' => 'ENUM("pdf","doc","txt","html")', '$.content_id' => 'INT', '$.price' => 'DECIMAL(10,2)']
     * @return void
     * @throws \Exception If the table could not be created (e.g., table already exists)
     */
    public function initialize(array $metadataJsonPathIndexes = []): void
    {
        // Build all SQL statements for single multi-query execution with proper escaping
        $binaryCodeLengthInBytes = ceil($this->dimension / 8);
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $engine = $this->escapeIdentifier($this->engine);

        $normalizedVectorLengthInBytes = 4 * $this->dimension; // float32 per component

        // If JSON path indexes are requested, create virtual generated columns + indexes
        $metadataIndexMap = [];
        $metadataColsIndexes = "";
        if (!empty($metadataJsonPathIndexes)) {

            if (!$this->supportsVirtualGeneratedColumns()) {
                throw new \Exception('Virtual generated columns with indexes require MySQL >= 5.7.6 or MariaDB >= 10.2.0');
            }

            $metadataColumns = [];
            $metadataIndexes = [];
            
            foreach ($metadataJsonPathIndexes as $path => $sqlTypeSpec) {
                // Validate JSON path
                $path = $this->validateJsonPath($path);

                // Validate SQL type
                $sqlType = trim((string)$sqlTypeSpec);
                if ($sqlType === '') {
                    throw new \InvalidArgumentException("Empty SQL type for metadata index path: {$path}");
                }

                // Derive column and index names from JSON path
                // Remove leading '$.', replace non-alphanumeric/underscore with underscores
                $colName = 'metadata_' . preg_replace('/[^A-Za-z0-9_]/', '_', substr($path, 2));
                $idxName = 'idx_' . $colName;
                $escapedColName = $this->escapeIdentifier($colName);
                $escapedIdxName = $this->escapeIdentifier($idxName);
                $escapedPath = $this->mysqli->real_escape_string($path);

                // Create generated column definition
                // Determine expression based on SQL type family (case-insensitive)
                $primitiveType = $this->detectPrimitiveType($sqlType);
                // Precompute common fragments
                $jx = "JSON_EXTRACT(`metadata`, '" . $escapedPath . "')";   // JSON value
                $ju = "JSON_UNQUOTE($jx)";                                  // string value
                switch ($primitiveType) {
                    case 'boolean':
                        // Map JSON booleans true/false to 1/0, fallback to numeric cast
                        $expr = "CASE $jx WHEN true THEN 1 WHEN false THEN 0 ELSE CAST($ju AS SIGNED) END";
                        break;
                    case 'integer':
                        $expr = "CAST($ju AS SIGNED)";
                        break;
                    case 'float':
                        $expr = "CAST($ju AS {$sqlType})";
                        break;
                    default: // string-like
                        $expr = $ju;
                        break;
                }
                $metadataColumns[] = "{$escapedColName} {$sqlType} GENERATED ALWAYS AS ({$expr}) VIRTUAL";

                // Create index definition
                if ($this->requiresIndexPrefixLength($sqlType)) { // determine if a prefix length is needed
                    $metadataIndexes[] = "INDEX {$escapedIdxName} ({$escapedColName}(" . self::INDEX_PREFIX_LENGTH . "))";
                } else {
                    $metadataIndexes[] = "INDEX {$escapedIdxName} ({$escapedColName})";
                }

                // Map JSON path to generated column info
                $metadataIndexMap[$path] = [
                    'column' => $colName,
                    'primitive_type' => $this->detectPrimitiveType($sqlType),
                ];
            }

            if (!empty($metadataColumns)) {
                $metadataColsIndexes .= ", " . implode(", ", $metadataColumns) . ", " . implode(", ", $metadataIndexes);
            }
        }

        $queries = "
            CREATE TABLE {$escapedVectorTableName} (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                normalized_vector VARBINARY({$normalizedVectorLengthInBytes}),
                binary_code VARBINARY({$binaryCodeLengthInBytes}),
                metadata JSON DEFAULT NULL
                {$metadataColsIndexes}
            ) ENGINE={$engine};
        ";

        if (!$this->mysqli->multi_query($queries)) {
            throw new \Exception("Failed to initialize table: " . $this->mysqli->error);
        }

        // Clear all results from multi-query
        do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());

        // Cache index map
        $this->metadataIndexMap = $metadataIndexMap;
    }

    /**
     * Compute the dot product between two equal-length numeric arrays using
     * a manual loop with x32 unrolling for performance on large vectors.
     */
    private function dot(array $a, array $b): float
    {
        $n = $this->dimension; // both arrays are validated to match this dimension
        $sum = 0.0;
        $i = 0;
        $limit = $n - ($n % 32);

        for (; $i < $limit; $i += 32) {
            $sum += ($a[$i] * $b[$i])
                  + ($a[$i + 1] * $b[$i + 1])
                  + ($a[$i + 2] * $b[$i + 2])
                  + ($a[$i + 3] * $b[$i + 3])
                  + ($a[$i + 4] * $b[$i + 4])
                  + ($a[$i + 5] * $b[$i + 5])
                  + ($a[$i + 6] * $b[$i + 6])
                  + ($a[$i + 7] * $b[$i + 7])
                  + ($a[$i + 8] * $b[$i + 8])
                  + ($a[$i + 9] * $b[$i + 9])
                  + ($a[$i + 10] * $b[$i + 10])
                  + ($a[$i + 11] * $b[$i + 11])
                  + ($a[$i + 12] * $b[$i + 12])
                  + ($a[$i + 13] * $b[$i + 13])
                  + ($a[$i + 14] * $b[$i + 14])
                  + ($a[$i + 15] * $b[$i + 15])
                  + ($a[$i + 16] * $b[$i + 16])
                  + ($a[$i + 17] * $b[$i + 17])
                  + ($a[$i + 18] * $b[$i + 18])
                  + ($a[$i + 19] * $b[$i + 19])
                  + ($a[$i + 20] * $b[$i + 20])
                  + ($a[$i + 21] * $b[$i + 21])
                  + ($a[$i + 22] * $b[$i + 22])
                  + ($a[$i + 23] * $b[$i + 23])
                  + ($a[$i + 24] * $b[$i + 24])
                  + ($a[$i + 25] * $b[$i + 25])
                  + ($a[$i + 26] * $b[$i + 26])
                  + ($a[$i + 27] * $b[$i + 27])
                  + ($a[$i + 28] * $b[$i + 28])
                  + ($a[$i + 29] * $b[$i + 29])
                  + ($a[$i + 30] * $b[$i + 30])
                  + ($a[$i + 31] * $b[$i + 31]);
        }

        for (; $i < $n; $i++) {
            $sum += $a[$i] * $b[$i];
        }

        return $sum;
    }

    /**
     * Clean up the vector table for this instance
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitialize(): void
    {
        // Drop table with proper escaping
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());

        $queries = "DROP TABLE IF EXISTS {$escapedVectorTableName};";

        if (!$this->mysqli->multi_query($queries)) {
            throw new \Exception("Failed to drop table: " . $this->mysqli->error);
        }

        // Clear all results from multi-query
        do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());

        // Invalidate cached index map
        $this->metadataIndexMap = null;
    }

    /**
     * Compute the cosine similarity between two vectors
     *
     * This method normalizes both input vectors and then computes their dot product,
     * which equals the cosine similarity for normalized vectors.
     *
     * @param array $v1 The first vector
     * @param array $v2 The second vector
     * @return float|null The cosine similarity between the two vectors [-1, 1], or null for invalid inputs
     * @throws \Exception
     */
    public function cosim(array $v1, array $v2): ?float
    {
        // Validate vector dimensions match
        if (count($v1) !== count($v2)) {
            throw new \InvalidArgumentException("Vector dimensions must match");
        }

        if (count($v1) !== $this->dimension) {
            throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
        }

        // Normalize both vectors before computing dot product (equals cosine similarity)
        $normalizedV1 = $this->normalize($v1);
        $normalizedV2 = $this->normalize($v2);

        return $this->dot($normalizedV1, $normalizedV2);
    }

    /**
     * Insert or update a vector
     * @param array $vector The vector to insert or update
     * @param array|null $metadata Optional metadata to store (JSON-serializable)
     * @param int|null $id Optional ID of the vector to update
     * @return int The ID of the inserted or updated vector
     * @throws \Exception If the vector could not be inserted or updated
     */
    public function upsert(array $vector, ?array $metadata = null, ?int $id = null): int
    {
        // Validate vector dimension
        if (count($vector) !== $this->dimension) {
            throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
        }

        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());

        $insertQuery = empty($id) ?
            "INSERT INTO {$escapedTableName} (normalized_vector, binary_code, metadata) VALUES (?, UNHEX(?), ?)" :
            "UPDATE {$escapedTableName} SET normalized_vector = ?, binary_code = UNHEX(?), metadata = ? WHERE id = ?";

        $statement = $this->mysqli->prepare($insertQuery);
        if(!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        try {
            $normalizedVectorBlob = $this->vectorToBlob($normalizedVector);
            $metadataJson = is_null($metadata) ? null : json_encode($metadata);

            if(empty($id)) {
                $statement->bind_param('sss', $normalizedVectorBlob, $binaryCode, $metadataJson);
            } else {
                $statement->bind_param('sssi', $normalizedVectorBlob, $binaryCode, $metadataJson, $id);
            }

            if(!$statement->execute()) {
                throw new \Exception("Execute failed: " . $statement->error);
            }

            return $statement->insert_id;
        } finally {
            $statement->close();
        }
    }

    /**
     * Insert multiple vectors (with optional per-item metadata) in a single transaction
     * @param array $vectorData Array of items: [ ['vector' => array<float>, 'metadata' => array|null], ... ]
     * @return array Array of ids of the inserted vectors
     * @throws \Exception
     */
    public function batchInsert(array $vectorData): array {
        $ids = [];

        $this->mysqli->begin_transaction();

        try {
            $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
            $statement = $this->mysqli->prepare("INSERT INTO {$escapedTableName} (normalized_vector, binary_code, metadata) VALUES (?, UNHEX(?), ?)");
            if(!$statement) {
                throw new \Exception("Prepare failed: " . $this->mysqli->error);
            }

            try {
                foreach ($vectorData as $item) {

                    if (!is_array($item) || !array_key_exists('vector', $item)) {
                        throw new \InvalidArgumentException('batchInsert expects each item to have a vector key');
                    }

                    $vector = $item['vector'];

                    if (count($vector) !== $this->dimension) {
                        throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
                    }

                    $normalizedVector = $this->normalize($vector);
                    $binaryCode = $this->vectorToHex($normalizedVector);
                    $normalizedVectorBlob = $this->vectorToBlob($normalizedVector);

                    $metadataJson = null;
                    if (array_key_exists('metadata', $item)) {
                        $metadata = $item['metadata'];
                        $metadataJson = is_null($metadata) ? null : json_encode($metadata);
                    }

                    $statement->bind_param('sss', $normalizedVectorBlob, $binaryCode, $metadataJson);

                    if (!$statement->execute()) {
                        throw new \Exception("Execute failed: " . $statement->error);
                    }

                    $ids[] = $statement->insert_id;
                }
            } finally {
                $statement->close();
            }

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        }

        return $ids;
    }

    /**
     * Select one or more vectors by id
     * @param \mysqli $mysqli The mysqli connection
     * @param array $ids The ids of the vectors to select
     * @return array Array of results, each containing:
     *               - 'id' (int)
     *               - 'metadata' (array|null)
     */
    public function select(array $ids): array {

        $placeholders = implode(', ', array_fill(0, count($ids), '?'));
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, metadata FROM {$escapedVectorTableName} WHERE id IN ({$placeholders})");
        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        try {
            $types = str_repeat('i', count($ids));
            $statement->bind_param($types, ...$ids);
            if (!$statement->execute()) {
                throw new \Exception("Execute failed: " . $statement->error);
            }
            $statement->bind_result($vectorId, $metadataJson);

            $result = [];
            while ($statement->fetch()) {
                $result[] = [
                    'id' => $vectorId,
                    'metadata' => is_null($metadataJson) ? null : json_decode($metadataJson, true),
                ];
            }
        } finally {
            $statement->close();
        }

        return $result;
    }

    /**
     * Select all vectors in the table
     * @return array Array of results, each containing:
     *               - 'id' (int)
     *               - 'metadata' (array|null)
     */
    public function selectAll(): array {

        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, metadata FROM {$escapedVectorTableName}");

        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        try {
            if (!$statement->execute()) {
                throw new \Exception("Execute failed: " . $statement->error);
            }
            $statement->bind_result($vectorId, $metadataJson);

            $result = [];
            while ($statement->fetch()) {
                $result[] = [
                    'id' => $vectorId,
                    'metadata' => is_null($metadataJson) ? null : json_decode($metadataJson, true),
                ];
            }
        } finally {
            $statement->close();
        }

        return $result;
    }

    /**
     * Select vectors filtered by JSON metadata using simple AND of equality conditions.
     *
     * Input shape: associative array of JSON path => value pairs, e.g.:
     *   ['$.content_type' => 'pdf', '$.content_id' => 456, '$.chunk_hash' => null]
     *
     * Semantics:
     * - Each entry is treated as equality on the exact JSON path key provided (must start with $)
     * - For value === null, this matches three cases:
     *     1) the path exists with an explicit JSON null value
     *     2) the path is missing from the JSON object
     *     3) the entire metadata column is NULL
     * - If a generated column index exists for a path, use it (fast) with type-aware binding based on metadata index map.
     * - Otherwise use JSON_EXTRACT fallback and infer JSON primitive type from PHP values.
     *
     * @param array $conditions Associative map of JSON paths to values (ANDed together)
     * @param int|null $limit Optional LIMIT
     * @return array Array of results, each containing:
     *               - 'id' (int)
     *               - 'metadata' (array|null)
     * @throws \Exception on SQL preparation/errors or invalid input
     */
    public function selectByMetadata(array $conditions, ?int $limit = null): array
    {
        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
        $metadataIndexMap = $this->getMetadataIndexMap();

        $clauses = [];
        $params = [];
        $types = '';
        foreach ($conditions as $path => $value) {
            // Validate JSON path
            $path = $this->validateJsonPath((string)$path);

            if (isset($metadataIndexMap[$path]) && is_array($metadataIndexMap[$path])) {
                // Use generated column index with type awareness
                $colInfo = $metadataIndexMap[$path];
                $col = $this->escapeIdentifier($colInfo['column']);
                $primitiveType = $colInfo['primitive_type'];
                if ($value === null) {
                    $clauses[] = "{$col} IS NULL";
                } else {
                    $bindVal = null; $bindType = null;
                    switch ($primitiveType) {
                        case 'integer':
                            if (!is_numeric($value)) {
                                throw new \InvalidArgumentException("Expected integer for {$path}");
                            }
                            $bindVal = (int)$value; $bindType = 'i';
                            break;
                        case 'float':
                            if (!is_numeric($value)) {
                                throw new \InvalidArgumentException("Expected float for {$path}");
                            }
                            $bindVal = (float)$value; $bindType = 'd';
                            break;
                        case 'boolean':
                            if (is_bool($value) || is_int($value)) {
                                $bindVal = (bool)$value ? 1 : 0;
                            } else {
                                $t = strtolower(trim((string)$value));
                                if ($t === 'true' || $t === '1') { $bindVal = 1; }
                                elseif ($t === 'false' || $t === '0') { $bindVal = 0; }
                                else {
                                    throw new \InvalidArgumentException("Expected boolean true/false or 0/1 for {$path}");
                                }
                            }
                            $bindType = 'i';
                            break;
                        case 'string':
                        default:
                            $bindVal = (string)$value; $bindType = 's';
                            break;
                    }
                    $clauses[] = "{$col} = ?";
                    $params[] = $bindVal; $types .= $bindType;
                }
            } else {
                // Fallback to JSON_EXTRACT: infer JSON primitive from PHP type
                $jx = "JSON_EXTRACT(metadata, ?)";
                if ($value === null) {
                    // Match explicit JSON null, missing path, or metadata NULL
                    $clauses[] = "($jx IS NULL OR JSON_TYPE($jx) = 'NULL')";
                    $params[] = $path; $types .= 's';
                    $params[] = $path; $types .= 's';
                } else {
                    $ju = "JSON_UNQUOTE($jx)";
                    if (is_int($value)) {
                        $clauses[] = "CAST($ju AS SIGNED) = ?";
                        $params[] = $path; $types .= 's';
                        $params[] = (int)$value; $types .= 'i';
                    } elseif (is_float($value)) {
                        // Fixed-scale DECIMAL equality to avoid DOUBLE exact-equality pitfalls
                        $precision = 38; $scale = 12; // reasonable default
                        $decStr = rtrim(rtrim(sprintf('%.12F', $value), '0'), '.');
                        $clauses[] = "CAST($ju AS DECIMAL({$precision},{$scale})) = CAST(? AS DECIMAL({$precision},{$scale}))";
                        $params[] = $path; $types .= 's';
                        $params[] = $decStr; $types .= 's';
                    } elseif (is_bool($value)) {
                        $clauses[] = "(CASE $jx WHEN true THEN 1 WHEN false THEN 0 ELSE CAST($ju AS SIGNED) END) = ?";
                        $params[] = $path; $types .= 's';
                        $params[] = $path; $types .= 's';
                        $params[] = ($value ? 1 : 0); $types .= 'i';
                    } elseif (is_string($value)) {
                        $clauses[] = "$ju = ?";
                        $params[] = $path; $types .= 's';
                        $params[] = $value; $types .= 's';
                    } else {
                        $clauses[] = "$ju = ?";
                        $params[] = $path; $types .= 's';
                        $params[] = (string)$value; $types .= 's';
                    }
                }
            }
        }
        
        if (empty($clauses)) {
            throw new \InvalidArgumentException('selectByMetadata requires at least one valid JSON path => value pair');
        }

        $where = implode(' AND ', $clauses);
        $sql = "SELECT id, metadata FROM {$escapedTableName} WHERE {$where}";
        $useLimit = $limit !== null && $limit > 0;
        if ($useLimit) {
            $sql .= ' LIMIT ?';
        }

        $stmt = $this->mysqli->prepare($sql);
        if (!$stmt) {
            throw new \Exception($this->mysqli->error);
        }
        try {
            if ($useLimit) {
                $types .= 'i';
                $params[] = $limit;
            }
            if ($types !== '') {
                $stmt->bind_param($types, ...$params);
            }
            if (!$stmt->execute()) {
                throw new \Exception("Execute failed: " . $stmt->error);
            }
            $stmt->bind_result($vectorId, $metadataJson);
            $result = [];
            while ($stmt->fetch()) {
                $result[] = [
                    'id' => $vectorId,
                    'metadata' => is_null($metadataJson) ? null : json_decode($metadataJson, true),
                ];
            }
        } finally {
            $stmt->close();
        }
        return $result;
    }

    /**
     * Returns the number of vectors stored in the database
     * @return int The number of vectors
     */
    public function count(): int {
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT COUNT(id) FROM {$escapedVectorTableName}");
        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }
        try {
            if (!$statement->execute()) {
                throw new \Exception("Execute failed: " . $statement->error);
            }
            $statement->bind_result($count);
            $statement->fetch();
            return (int)$count;
        } finally {
            $statement->close();
        }
    }

    /**
     * Calculate the Euclidean magnitude (L2 norm) of a vector
     *
     * @param array $vector Input vector
     * @return float The magnitude ||v|| = sqrt(v₁² + v₂² + ... + vₙ²)
     */
    private function getMagnitude(array $vector): float
    {
        $sumOfSquares = 0.0;
        foreach ($vector as $value) {
            $sumOfSquares += $value * $value;
        }
        return sqrt($sumOfSquares);
    }

    /**
     * Find vectors most similar to the given query vector using two-stage search
     *
     * Uses a two-stage algorithm for efficient similarity search:
     * 1. Binary quantization with Hamming distance for fast filtering
     * 2. Precise cosine similarity re-ranking of candidates
     *
     * @param array $vector Query vector to search for
     * @param int $n Maximum number of results to return (default: 10)
     * @param int|null $candidateMultiplier Optional multiplier for Stage 1 filtering accuracy.
     *                                      When null (default), uses adaptive formula based on result count:
     *                                      - Small result sets (n=1-5): 20x multiplier for maximum accuracy
     *                                      - Medium result sets (n=6-33): 3-20x adaptive multiplier
     *                                      - Large result sets (n>33): 3x minimum multiplier for efficiency
     *                                      This balances accuracy vs performance. When provided, uses the
     *                                      specified multiplier directly. Must be positive.
     * @return array Array of results, each containing:
     *               - 'id': Vector ID
     *               - 'similarity': Cosine similarity [-1, 1]
     *               - 'metadata': Associated metadata (array|null)
     * @throws \Exception If database operations fail or invalid input
     */
    public function search(array $vector, int $n = 10, ?int $candidateMultiplier = null): array
    {
        // Input validation
        if (empty($vector)) {
            throw new \InvalidArgumentException("Search vector cannot be empty");
        }

        if (count($vector) !== $this->dimension) {
            throw new \InvalidArgumentException("Search vector dimension must match table dimension: {$this->dimension}");
        }

        if ($n <= 0) {
            throw new \InvalidArgumentException("Number of results must be positive");
        }

        if ($candidateMultiplier !== null && $candidateMultiplier <= 0) {
            throw new \InvalidArgumentException("Candidate multiplier must be positive when provided");
        }

        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
        $queryVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($queryVector);

        // Determine candidate multiplier and limit
        if ($candidateMultiplier === null) {
            $candidateMultiplier = max(3, min(20, (int)ceil(100 / $n)));
        }
        $candidateLimit = $n * $candidateMultiplier;

        // Fetch top candidate IDs by Hamming distance (minimize I/O during filesort by keeping buffer size small)
        $sql = "SELECT id FROM {$escapedTableName} ORDER BY BIT_COUNT(binary_code ^ UNHEX(?)) LIMIT ?";
        $statement = $this->mysqli->prepare($sql);
        if (!$statement) {
            throw new \Exception("Failed to prepare search query: " . $this->mysqli->error);
        }
        $candidateIds = [];
        try {
            $statement->bind_param('si', $binaryCode, $candidateLimit);
            if (!$statement->execute()) {
                throw new \Exception("Execute failed: " . $statement->error);
            }
            $statement->bind_result($candidateId);
            while ($statement->fetch()) {
                $candidateIds[] = (int)$candidateId;
            }
        } finally {
            $statement->close();
        }

        // Avoid invalid SQL
        if (empty($candidateIds)) {
            return [];
        }

        // Fetch vectors only for those candidate IDs
        $ids = [];
        foreach ($candidateIds as $cid) { $ids[] = (int)$cid; }
        $sqlVectors = "SELECT id, normalized_vector FROM {$escapedTableName} WHERE id IN (" . implode(',', $ids) . ")";
        $stmtVectors = $this->mysqli->prepare($sqlVectors);
        if (!$stmtVectors) {
            throw new \Exception("Failed to prepare vector fetch query: " . $this->mysqli->error);
        }
        try {
            if (!$stmtVectors->execute()) {
                throw new \Exception("Execute failed: " . $stmtVectors->error);
            }
            $stmtVectors->bind_result($vectorId, $normalizedVectorBlob);
            $results = [];
            while ($stmtVectors->fetch()) {
                $vec = $this->blobToVector($normalizedVectorBlob);
                $results[(int)$vectorId] = [
                    'id' => (int)$vectorId,
                    'similarity' => $this->dot($vec, $queryVector), // compute similarity for PHP-side re-ranking
                ];
            }
        } finally {
            $stmtVectors->close();
        }

        // PHP-side re-ranking
        uasort($results, static function($a, $b) {
            if ($a['similarity'] === $b['similarity']) return 0;
            return ($a['similarity'] < $b['similarity']) ? 1 : -1;
        });

        // Keep top-N vectors
        if (count($results) > $n) {
            $results = array_slice($results, 0, $n, true);
        }

        // Avoid invalid SQL
        if (empty($results)) {
            return [];
        }

        // Fetch metadata for top-N only
        $ids = [];
        foreach ($results as $r) { $ids[] = (int)$r['id']; }
        $sqlMeta = "SELECT id, metadata FROM {$escapedTableName} WHERE id IN (" . implode(',', $ids) . ")";
        $stmtMeta = $this->mysqli->prepare($sqlMeta);
        if (!$stmtMeta) {
            throw new \Exception("Failed to prepare metadata query: " . $this->mysqli->error);
        }
        try {
            if (!$stmtMeta->execute()) {
                throw new \Exception("Execute failed: " . $stmtMeta->error);
            }
            $stmtMeta->bind_result($mid, $metadataJson);
            while ($stmtMeta->fetch()) {
                $results[(int)$mid]['metadata'] = is_null($metadataJson) ? null : json_decode($metadataJson, true);
            }
        } finally {
            $stmtMeta->close();
        }

        return array_values($results);
    }

    /**
     * Normalize a vector to unit length (L2 normalization)
     *
     * Converts a vector to unit length while preserving direction.
     * For zero vectors, uses epsilon to avoid division by zero.
     *
     * @param array $vector Input vector to normalize
     * @return array Normalized vector with magnitude ≈ 1.0
     */
    public function normalize(array $vector): array
    {
        // Calculate magnitude
        $magnitude = $this->getMagnitude($vector);

        // Small value to use for zero vectors (default: 1e-12)
        $epsilon = floatval(1e-12);

        // Handle zero and near-zero vectors with epsilon to avoid division by very small numbers
        if (abs($magnitude) < $epsilon) {
            $magnitude = $epsilon;
        }

        // Normalize: v_normalized = v / ||v||
        return array_map(fn($component) => $component / $magnitude, $vector);
    }

    /**
     * Remove a vector from the database
     * @param int $id The id of the vector to remove
     * @return void
     * @throws \Exception
     */
    public function delete(int $id): void {
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("DELETE FROM {$escapedVectorTableName} WHERE id = ?");
        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }
        try {
            $statement->bind_param('i', $id);
            if (!$statement->execute()) {
                throw new \Exception($statement->error);
            }
        } finally {
            $statement->close();
        }
    }

    public function getConnection(): \mysqli {
        return $this->mysqli;
    }
}