<?php

namespace MHz\MysqlVector;

class VectorTable
{
    private string $name;
    private int $dimension;
    private string $engine;
    private \mysqli $mysqli;

    const SQL_DOT_PRODUCT_FUNCTION = "
        CREATE FUNCTION MV_DOT_PRODUCT(v1 JSON, v2 JSON)
        RETURNS FLOAT
        DETERMINISTIC
        READS SQL DATA
        BEGIN
            DECLARE sim FLOAT DEFAULT 0;
            DECLARE i INT DEFAULT 0;
            DECLARE len INT DEFAULT JSON_LENGTH(v1);

            WHILE i < len DO
                SET sim = sim + (JSON_EXTRACT(v1, CONCAT('$[', i, ']')) * JSON_EXTRACT(v2, CONCAT('$[', i, ']')));
                SET i = i + 1;
            END WHILE;

            RETURN sim;
        END";

    /**
     * Instantiate a new VectorTable object.
     * @param \mysqli $mysqli The mysqli connection
     * @param string $name Name of the table.
     * @param int $dimension Dimension of the vectors.
     * @param string $engine The storage engine to use for the tables
     * @throws \InvalidArgumentException If dimension exceeds maximum supported value
     */
    public function __construct(\mysqli $mysqli, string $name, int $dimension = 384, string $engine = 'InnoDB')
    {
        // Maximum dimensions are limited by VARBINARY storage
        // binary_code uses VARBINARY(ceil(dimension/8)); VARBINARY max length in MySQL is 65,535 bytes
        // maximum supported dimensions = 65,535 * 8 = 524,280.
        $maxDimensions = 65535 * 8;

        if ($dimension <= 0) {
            throw new \InvalidArgumentException("Dimension must be a positive integer, got $dimension");
        }

        if ($dimension > $maxDimensions) {
            throw new \InvalidArgumentException("Maximum supported dimension is $maxDimensions, got $dimension");
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
     * Initialize only the tables for this VectorTable instance
     * @param bool $ifNotExists Whether to use IF NOT EXISTS in the CREATE TABLE statements
     * @return void
     * @throws \Exception If the tables could not be created
     */
    public function initializeTables(bool $ifNotExists = true): void
    {
        $this->mysqli->begin_transaction();

        try {
            // Build all SQL statements for single multi-query execution with proper escaping
            $binaryCodeLengthInBytes = ceil($this->dimension / 8);
            $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
            $engine = $this->escapeIdentifier($this->engine);

            $ifNotExistsClause = $ifNotExists ? 'IF NOT EXISTS' : '';

            // Execute all statements in single multi-query
            $queries = "
                CREATE TABLE {$ifNotExistsClause} {$escapedVectorTableName} (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    normalized_vector JSON,
                    binary_code VARBINARY({$binaryCodeLengthInBytes}),
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE={$engine};
            ";

            if (!$this->mysqli->multi_query($queries)) {
                throw new \Exception("Failed to initialize tables: " . $this->mysqli->error);
            }

            // Clear all results from multi-query
            do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Initialize global MySQL functions (should be called once per database)
     * @param \mysqli $mysqli Database connection
     * @return void
     * @throws \Exception If the functions could not be created
     */
    public static function initializeFunctions(\mysqli $mysqli): void
    {
        $mysqli->begin_transaction();

        try {
            // Execute all DROP and CREATE statements in a single multi-statement query
            $queries = "
                DROP FUNCTION IF EXISTS MV_DOT_PRODUCT;
                " . self::SQL_DOT_PRODUCT_FUNCTION . ";
            ";

            if (!$mysqli->multi_query($queries)) {
                throw new \Exception("Failed to initialize functions: " . $mysqli->error);
            }

            // Clear all results from multi-query
            do { if ($result = $mysqli->store_result()) { $result->free(); } } while ($mysqli->next_result());

            $mysqli->commit();
        } catch (\Exception $e) {
            $mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Create the tables and functions required for storing vectors
     * @param bool $ifNotExists Whether to use IF NOT EXISTS in the CREATE TABLE statements
     * @return void
     * @throws \Exception If the tables or functions could not be created
     */
    public function initialize(bool $ifNotExists = true): void
    {
        // Initialize functions first (global)
        self::initializeFunctions($this->mysqli);

        // Then initialize tables (instance-specific)
        $this->initializeTables($ifNotExists);
    }

    /**
     * Clean up tables for this VectorTable instance
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitializeTables(): void
    {
        $this->mysqli->begin_transaction();

        try {
            // Drop table with proper escaping
            $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());

            $queries = "DROP TABLE IF EXISTS {$escapedVectorTableName};";

            if (!$this->mysqli->multi_query($queries)) {
                throw new \Exception("Failed to drop tables: " . $this->mysqli->error);
            }

            // Clear all results from multi-query
            do { if ($result = $this->mysqli->store_result()) { $result->free(); } } while ($this->mysqli->next_result());

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Clean up global MySQL functions
     * @param \mysqli $mysqli Database connection
     * @return void
     * @throws \Exception If cleanup fails
     */
    public static function deinitializeFunctions(\mysqli $mysqli): void
    {
        $mysqli->begin_transaction();

        try {
            // Drop all functions in single multi-query
            $queries = "DROP FUNCTION IF EXISTS MV_DOT_PRODUCT;";

            if (!$mysqli->multi_query($queries)) {
                throw new \Exception("Failed to drop functions: " . $mysqli->error);
            }

            // Clear all results from multi-query
            do { if ($result = $mysqli->store_result()) { $result->free(); } } while ($mysqli->next_result());

            $mysqli->commit();
        } catch (\Exception $e) {
            $mysqli->rollback();
            throw $e;
        }
    }

    /**
     * Complete cleanup of tables and functions
     * @return void
     * @throws \Exception If cleanup fails
     */
    public function deinitialize(): void
    {
        // Clean up tables first
        $this->deinitializeTables();

        // Then clean up functions (global)
        self::deinitializeFunctions($this->mysqli);
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

        $statement = $this->mysqli->prepare("SELECT MV_DOT_PRODUCT(?, ?)");

        if(!$statement) {
            throw new \Exception("Failed to prepare dot product query: " . $this->mysqli->error);
        }

        // Normalize both vectors before computing dot product (which equals cosine similarity)
        $normalizedV1 = json_encode($this->normalize($v1));
        $normalizedV2 = json_encode($this->normalize($v2));

        $statement->bind_param('ss', $normalizedV1, $normalizedV2);
        $statement->execute();
        $statement->bind_result($similarity);
        $statement->fetch();
        $statement->close();

        return $similarity;
    }

    /**
     * Insert or update a vector
     * @param array $vector The vector to insert or update
     * @param int|null $id Optional ID of the vector to update
     * @return int The ID of the inserted or updated vector
     * @throws \Exception If the vector could not be inserted or updated
     */
    public function upsert(array $vector, ?int $id = null): int
    {
        // Validate vector dimension
        if (count($vector) !== $this->dimension) {
            throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
        }

        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());

        $insertQuery = empty($id) ?
            "INSERT INTO {$escapedTableName} (normalized_vector, binary_code) VALUES (?, UNHEX(?))" :
            "UPDATE {$escapedTableName} SET normalized_vector = ?, binary_code = UNHEX(?) WHERE id = ?";

        $statement = $this->mysqli->prepare($insertQuery);
        if(!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        $normalizedVectorJson = json_encode($normalizedVector);

        if(empty($id)) {
            $statement->bind_param('ss', $normalizedVectorJson, $binaryCode);
        } else {
            $statement->bind_param('ssi', $normalizedVectorJson, $binaryCode, $id);
        }

        $success = $statement->execute();
        if(!$success) {
            throw new \Exception($statement->error);
        }

        $id = $statement->insert_id;
        $statement->close();

        return $id;
    }

    /**
     * Insert multiple vectors in a single query
     * @param array $vectorArray Array of vectors to insert
     * @return array Array of ids of the inserted vectors
     * @throws \Exception
     */
    public function batchInsert(array $vectorArray): array {
        $ids = [];

        $this->mysqli->begin_transaction();

        try {
            $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
            $statement = $this->mysqli->prepare("INSERT INTO {$escapedTableName} (normalized_vector, binary_code) VALUES (?, UNHEX(?))");
            if(!$statement) {
                throw new \Exception("Prepare failed: " . $this->mysqli->error);
            }

            foreach ($vectorArray as $vector) {
                // Validate vector dimension
                if (count($vector) !== $this->dimension) {
                    throw new \InvalidArgumentException("Vector dimension must match table dimension: {$this->dimension}");
                }

                $normalizedVector = $this->normalize($vector);
                $binaryCode = $this->vectorToHex($normalizedVector);
                $normalizedVectorJson = json_encode($normalizedVector);

                $statement->bind_param('ss', $normalizedVectorJson, $binaryCode);

                if (!$statement->execute()) {
                    throw new \Exception("Execute failed: " . $statement->error);
                }

                $ids[] = $statement->insert_id;
            }

            $this->mysqli->commit();
        } catch (\Exception $e) {
            $this->mysqli->rollback();
            throw $e;
        } finally {
            $statement->close();
        }

        return $ids;
    }

    /**
     * Select one or more vectors by id
     * @param \mysqli $mysqli The mysqli connection
     * @param array $ids The ids of the vectors to select
     * @return array Array of vectors
     */
    public function select(array $ids): array {

        $placeholders = implode(', ', array_fill(0, count($ids), '?'));
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM {$escapedVectorTableName} WHERE id IN ({$placeholders})");
        $types = str_repeat('i', count($ids));

        $refs = [];
        foreach ($ids as $key => &$id) {
            $refs[$key] = &$id;
        }
        unset($id);

        call_user_func_array([$statement, 'bind_param'], array_merge([$types], $refs));
        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVector, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => json_decode($normalizedVector, true),
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    public function selectAll(): array {

        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT id, normalized_vector, binary_code FROM {$escapedVectorTableName}");

        if (!$statement) {
            throw new \Exception($this->mysqli->error);
        }

        $statement->execute();
        $statement->bind_result($vectorId, $normalizedVector, $binaryCode);

        $result = [];
        while ($statement->fetch()) {
            $result[] = [
                'id' => $vectorId,
                'normalized_vector' => json_decode($normalizedVector, true),
                'binary_code' => $binaryCode
            ];
        }

        $statement->close();

        return $result;
    }

    /**
     * Returns the number of vectors stored in the database
     * @return int The number of vectors
     */
    public function count(): int {
        $escapedVectorTableName = $this->escapeIdentifier($this->getVectorTableName());
        $statement = $this->mysqli->prepare("SELECT COUNT(id) FROM {$escapedVectorTableName}");
        $statement->execute();
        $statement->bind_result($count);
        $statement->fetch();
        $statement->close();
        return $count;
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
     * 1. Stage 1: Binary quantization with Hamming distance for fast filtering
     * 2. Stage 2: Precise cosine similarity re-ranking of candidates
     *
     * @param array $vector Query vector to search for
     * @param int $n Maximum number of results to return (default: 10)
     * @return array Array of results, each containing:
     *               - 'id': Vector ID
     *               - 'normalized_vector': L2-normalized vector
     *               - 'similarity': Cosine similarity [-1, 1]
     * @throws \Exception If database operations fail or invalid input
     */
    public function search(array $vector, int $n = 10): array
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

        $escapedTableName = $this->escapeIdentifier($this->getVectorTableName());
        $normalizedVector = $this->normalize($vector);
        $binaryCode = $this->vectorToHex($normalizedVector);
        $normalizedVectorJson = json_encode($normalizedVector);

        $sql = "
        SELECT
            candidates.id,
            candidates.normalized_vector,
            MV_DOT_PRODUCT(candidates.normalized_vector, ?) AS similarity
        FROM (
            SELECT
                id,
                normalized_vector,
                BIT_COUNT(binary_code ^ UNHEX(?)) AS hamming_distance
            FROM {$escapedTableName}
            ORDER BY hamming_distance
            LIMIT ?
        ) AS candidates
        ORDER BY similarity DESC
        LIMIT ?";

        $statement = $this->mysqli->prepare($sql);

        if (!$statement) {
            throw new \Exception("Failed to prepare search query: " . $this->mysqli->error);
        }

        $statement->bind_param('ssii', $normalizedVectorJson, $binaryCode, $n, $n);
        $statement->execute();
        $statement->bind_result($id, $nv, $sim);

        $results = [];
        while ($statement->fetch()) {
            $results[] = [
                'id' => $id,
                'normalized_vector' => json_decode($nv, true),
                'similarity' => $sim
            ];
        }

        $statement->close();

        return $results;
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
        $statement->bind_param('i', $id);
        $success = $statement->execute();
        if(!$success) {
            throw new \Exception($statement->error);
        }
        $statement->close();
    }

    public function getConnection(): \mysqli {
        return $this->mysqli;
    }
}